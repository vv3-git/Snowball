"""
sql_processor.py

SQL processing operations including SQLFluff, notebook generation, and SQL transformations.
"""
import os
import re
import subprocess
import time
import shutil
from tqdm import tqdm
import nbformat as nbf
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell
from config import *


class SQLProcessor:
    def __init__(self):
        self.compiled_dir = os.path.join(project_dir, "target", "compiled")
        self.notebooks_dir = os.path.join(output_dir, "notebooks")
        
        term_width = shutil.get_terminal_size().columns
        self.bar_width = term_width // 4

    def run_sqlfluff_on_directory(self, directory_path, project_root):
        """
        Run SQLFluff fix on all SQL files in a directory.
        """
        try:
            sql_files = []
            for root, _, files in os.walk(directory_path):
                for file in files:
                    if file.endswith('.sql'):
                        sql_files.append(os.path.join(root, file))
            
            if not sql_files:
                return True
            
            success_count = 0
            with tqdm(total=len(sql_files), desc="Applying SQLFluff", colour="green",
                     bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(self.bar_width) + '}|') as pbar:
                for sql_file in sql_files:
                    try:
                        result = subprocess.run(
                            ["sqlfluff", "fix", "--force", sql_file],
                            check=False,
                            text=True,
                            capture_output=True,
                            cwd=project_root
                        )

                        if result.returncode == 0:
                            success_count += 1
                            
                    except Exception:
                        pass
                    
                    pbar.update(1)
            
            return success_count > 0
                
        except Exception:
            return False

    def apply_sqlfluff_to_compiled(self, project_root):
        """
        Apply SQLFluff to all compiled SQL files before packaging.
        """
        try:
            subprocess.run(["sqlfluff", "--version"], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            with tqdm(desc="❌ SQLFluff not available", bar_format='{desc}') as pbar:
                time.sleep(1)
            return False
        
        return self.run_sqlfluff_on_directory(self.compiled_dir, project_root)

    def generate_notebooks(self):
        """Generate Jupyter notebooks from compiled SQL by model folder"""
        try:
            os.makedirs(self.notebooks_dir, exist_ok=True)

            model_folders = set()
            for root, _, files in os.walk(self.compiled_dir):
                for file in files:
                    if file.endswith('.sql'):
                        rel_path = os.path.relpath(root, self.compiled_dir)
                        if rel_path != '.':
                            folder_name = rel_path.split(os.sep)[-1]
                            if folder_name != "models":
                                model_folders.add(folder_name)

            with tqdm(total=len(model_folders), color="green", desc="📓 Generating notebooks", 
                     bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(self.bar_width) + '}|') as pbar:
                for folder in model_folders:
                    if folder == "models":
                        continue
                        
                    notebook_path = os.path.join(self.notebooks_dir, f"{folder}_nb.ipynb")
                    nb = new_notebook()
                    
                    folder_name = folder.upper().split('_')[-1]
                    nb.cells.append(new_markdown_cell(
                        "## SNOWBALL Spark SQL version\n"
                        f"#### **Notebook to create {folder_name} layer**\n"
                        f"##### **Creating {folder_name} schema to create required {folder_name} tables**\n"
                    ))
                    nb.cells.append(new_code_cell(f"%%sql\nCREATE SCHEMA IF NOT EXISTS {folder.split('_')[-1]};"))

                    if folder == 'tests':
                        folder_path = os.path.join(self.compiled_dir, 'Snowball_dbt', folder)
                    else:            
                        folder_path = os.path.join(self.compiled_dir, 'Snowball_dbt', 'models', folder)

                    for root, _, files in os.walk(folder_path):
                        for file in sorted(files):
                            if file.endswith('.sql'):
                                file_path = os.path.join(root, file)
                                model_name = os.path.splitext(file)[0]
                                
                                with open(file_path, 'r', encoding='utf-8') as f:
                                    sql_content = f.read()                    
                                nb.cells.append(new_markdown_cell(f"##### **{model_name}**"))
                                nb.cells.append(new_code_cell(
                                    f"%%sql\n"
                                    f"DROP TABLE IF EXISTS {folder.split('_')[-1]}.{model_name};\n"
                                    f"CREATE TABLE {folder.split('_')[-1]}.{model_name} AS\n"
                                    f"{sql_content}"
                                ))
                    with open(notebook_path, 'w', encoding='utf-8') as f:
                        nbf.write(nb, f)
                        
                    pbar.update(1)
                    
        except Exception:
            return False
        return True

    def transform_compiled_sql(self, sql_file_path):
        """Post-process a compiled SQL file to wrap in stored procedure format."""
        try:
            with open(sql_file_path, "r", encoding="utf-8") as f:
                sql_code = f.read()

            rel_path = os.path.relpath(sql_file_path, self.compiled_dir)
            parts = rel_path.split(os.sep)

            if "models" in parts:
                models_idx = parts.index("models")
                if models_idx + 1 < len(parts):
                    folder_raw = parts[models_idx + 1]
                    model_folder_name = folder_raw.split("_", 1)[1] if "_" in folder_raw else folder_raw
                else:
                    return
            else:
                return

            model_name = os.path.splitext(parts[-1])[0]

            proc_header = (
                f"CREATE OR ALTER PROCEDURE {model_folder_name}.sp_{model_name}\n"
                f"AS\nBEGIN\n    SET NOCOUNT ON;\n\n"
                f"    BEGIN\n        DROP TABLE IF EXISTS {model_folder_name}.{model_name}; \n    END;\n\n"
            )

            match = list(re.finditer(r"\bFROM\b", sql_code, re.IGNORECASE))
            if match:
                last_from = match[-1]
                insert_pos = last_from.start()
                sql_code = sql_code[:insert_pos] + f"INTO {model_folder_name}.{model_name}\n" + sql_code[insert_pos:]

            sql_code = proc_header + sql_code.strip() + "\nEND;"

            with open(sql_file_path, "w", encoding="utf-8") as f:
                f.write(sql_code)

        except Exception:
            pass

    def process_compiled_sql_files(self):
        """Walk through compiled models directory and transform all SQL files."""
        sql_files = []
        for root, _, files in os.walk(self.compiled_dir):
            for file in files:
                if file.endswith(".sql"):
                    sql_files.append(os.path.join(root, file))
        
        with tqdm(total=len(sql_files), desc="Transforming SQL files", colour="green", 
                 bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(self.bar_width) + '}|') as pbar:
            for sql_file in sql_files:
                self.transform_compiled_sql(sql_file)
                pbar.update(1)

    def update_revenue_model_with_table_name(self, output_dir, table_name):
        """
        Replace occurrences of '.snowball_revenue' with the provided table_name
        in all compiled revenue.sql model files.
        """
        for root, dirs, files in os.walk(output_dir):
            for file in files:
                if file.lower() == "revenue.sql":
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            content = f.read()
                        updated_content = content.replace(".snowball_revenue", f".{table_name}")
                        if updated_content != content:
                            with open(file_path, "w", encoding="utf-8") as f:
                                f.write(updated_content)
                    except Exception as e:
                        print(f"⚠️ Error updating {file_path}: {e}")