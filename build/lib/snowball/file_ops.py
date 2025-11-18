"""
file_ops.py

File operations including zip, copy, directory management, and Git operations.
"""
import os
import zipfile
import shutil
import time
from pathlib import Path
from tqdm import tqdm
from config import *


class FileOperations:
    def __init__(self):
        self.compiled_dir = os.path.join(project_dir, "target", "compiled")
        self.dbt_seed_dir = os.path.join(project_dir, "seeds")
        self.profiles_dir = str(Path.home() / "Downloads/snowball_dbt")
        self.notebooks_dir = os.path.join(output_dir, "notebooks")
        
        term_width = shutil.get_terminal_size().columns
        self.bar_width = term_width // 4

    def remove_readonly_files(self, func, path, _):
        """Error handler for removing read-only files on Windows"""
        import stat
        os.chmod(path, stat.S_IWRITE)
        func(path)

    def clone_repo(self, source_path: str) -> str:
        """
        Clone the Git repository from source path.
        """
        repo_name = os.path.basename(source_path)
        downloads_dir = Path.home() / "Downloads"
        temp_clone_location = downloads_dir / f"{repo_name}_temp"
        final_clone_location = downloads_dir / repo_name

        # Remove existing target folder if present
        if final_clone_location.exists():
            shutil.rmtree(final_clone_location, onerror=self.remove_readonly_files)
        if temp_clone_location.exists():
            shutil.rmtree(temp_clone_location, onerror=self.remove_readonly_files)

        # Copy all files to a temporary folder first
        shutil.copytree(source_path, temp_clone_location, dirs_exist_ok=True)
        temp_clone_location.rename(final_clone_location)

        current_time = time.time()
        os.utime(final_clone_location, (current_time, current_time))

        # Return the mapping file path
        return str(final_clone_location / "seeds" / "column_mapping.csv")

    def copy_csv_to_downloads(self, src_csv_path: str) -> str:
        """
        Copy a CSV file from src_csv_path to the Downloads folder.
        """
        downloads_dir = Path.home() / "Downloads"
        downloads_dir.mkdir(parents=True, exist_ok=True)
        
        src_path = Path(src_csv_path)
        if not src_path.is_file():
            raise FileNotFoundError(f"Source CSV file not found: {src_csv_path}")
        
        dest_path = downloads_dir / src_path.name
        shutil.copy(src_path, dest_path)
        
        try:
            os.chmod(dest_path, 0o666)
        except Exception as e:
            print(f"Warning: Could not set permissions on {dest_path}: {e}")
        
        return str(dest_path)

    def copy_profiles_to_downloads(self, profiles_src_path: str) -> str:
        """
        Copy profiles.yml to the user's Downloads folder.
        """
        downloads_dir = Path.home() / "Downloads"
        downloads_dir.mkdir(parents=True, exist_ok=True)

        src_path = Path(profiles_src_path)
        if not src_path.is_file():
            raise FileNotFoundError(f"profiles.yml not found at: {profiles_src_path}")

        dest_path = downloads_dir / "profiles.yml"
        shutil.copy(src_path, dest_path)

        try:
            os.chmod(dest_path, 0o666)
        except Exception as e:
            print(f"Warning: Could not set permissions on {dest_path}: {e}")

        return str(dest_path)

    def zip_directory(self, source_dir, zip_path):
        """Zip the contents of an entire directory"""
        # Count total files first
        total_files = 0
        for root, _, files in os.walk(source_dir):
            total_files += len(files)
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            with tqdm(total=total_files, desc="Creating archive", colour="green", 
                     bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(self.bar_width) + '}|') as pbar:
                for root, _, files in os.walk(source_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, source_dir)
                        zipf.write(file_path, arcname)
                        pbar.update(1)

    def copy_seed_file(self, seed_path, target_dir, dbname, schemaname, tablename):
        """
        Copy the column_mapping.csv file to the dbt seeds directory.
        """
        try:
            shutil.copy(seed_path, target_dir)
            # Run dbt seed to update - we'll handle this in the main flow
            return True

        except FileNotFoundError:
            with tqdm(desc="❌ Mapping file not found", bar_format='{desc}') as pbar:
                time.sleep(1)
            return False
        except Exception as e:
            with tqdm(desc=f"❌ Failed to process mapping file: {e}", bar_format='{desc}') as pbar:
                time.sleep(1)
            return False

    def update_profile(self, profile_src_path, profiles_dir):
        """
        Copy profiles.yml from source to the dbt profiles directory.
        """
        try:
            shutil.copy(profile_src_path, profiles_dir)
            return True
        except FileNotFoundError:
            with tqdm(desc="❌ profiles.yml not found", bar_format='{desc}') as pbar:
                time.sleep(1)
            return False
        except Exception as e:
            with tqdm(desc=f"❌ Failed to update profiles.yml: {e}", bar_format='{desc}') as pbar:
                time.sleep(1)
            return False

    def cleanup_previous_run(self, cleaning_list):
        """Clean up previous compiled files and notebooks"""
        for dir_path in cleaning_list:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)