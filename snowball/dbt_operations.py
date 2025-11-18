"""
dbt_operations.py

All dbt-related operations including running commands, compilation, and progress tracking.
"""
import os
import re
import json
import time
import shutil
import threading
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
from tqdm import tqdm
from dbt.cli.main import dbtRunner
from config import *


class DbtOperations:
    def __init__(self):
        self.compiled_dir = os.path.join(project_dir, "target", "compiled")
        self.profiles_dir = str(Path.home() / "Downloads/snowball_dbt")
        
        term_width = shutil.get_terminal_size().columns
        self.bar_width = term_width // 4

    def get_dbt_models_count(self):
        """Count the number of dbt models in the project"""
        models_dir = os.path.join(project_dir, "models")
        model_count = 0
        if os.path.exists(models_dir):
            for root, _, files in os.walk(models_dir):
                model_count += len([f for f in files if f.endswith('.sql')])
        return max(model_count, 1)

    def _build_vars_string(self, dbname, schemaname, tablename):
        vars_dict = {
            'my_database': dbname,
            'my_schema': schemaname,
            'my_table': tablename
        }
        return json.dumps(vars_dict)

    def run_dbt_deps(self, dbname, schemaname, tablename):
        """Run dbt deps to install dependencies with realistic progress tracking"""
        vars_str = self._build_vars_string(dbname, schemaname, tablename)
        deps_args = [
            "deps",
            "--project-dir", project_dir,
            "--profiles-dir", self.profiles_dir,
            "--vars", vars_str
        ]
        
        with tqdm(
            total=100,
            desc="Installing dependencies",
            colour="green",
            bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(self.bar_width) + '}|'
        ) as pbar:
            
            pbar.update(15)
            pbar.set_description("🔍 Discovering packages")
            time.sleep(0.5)
            
            pbar.update(15)
            pbar.set_description("📖 Reading packages.yml")
            time.sleep(0.5)
            
            pbar.update(20)
            pbar.set_description("📥 Downloading packages")
            
            dbt = dbtRunner()
            stdout_capture = StringIO()
            stderr_capture = StringIO()
            
            start_time = time.time()
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                result = dbt.invoke(deps_args)
            execution_time = time.time() - start_time
            
            current_progress = 50
            
            if execution_time < 1:
                pbar.update(50)
                pbar.set_description("✅ Dependencies installed")
            else:
                installation_steps = [
                    ("🔗 Resolving dependencies", 15),
                    ("📦 Installing packages", 20),
                    ("✅ Verifying installations", 15)
                ]
                
                step_time = execution_time / len(installation_steps)
                
                for step_desc, step_progress in installation_steps:
                    pbar.set_description(step_desc)
                    time.sleep(step_time * 0.7)
                    current_progress += step_progress
                    pbar.update(step_progress)
            
            if pbar.n < 100:
                pbar.update(100 - pbar.n)
            
            if result.success:
                pbar.set_description("✅ Dependencies installed")
            else:
                pbar.set_description("❌ Dependencies failed")
        
        return result

    def run_dbt_seed(self, dbname, schemaname, tablename):
        """Run dbt seed to update the user mapping file with realistic progress tracking"""
        vars_str = self._build_vars_string(dbname, schemaname, tablename)
        seed_args = [
            "seed",
            "--project-dir", project_dir,
            "--profiles-dir", self.profiles_dir,
            "--vars", vars_str
        ]
        
        with tqdm(
            total=100,
            desc="Updating mapping file",
            colour="green",
            bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(self.bar_width) + '}|'
        ) as pbar:
            
            pbar.update(10)
            pbar.set_description("🔧 Initializing seed")
            time.sleep(0.3)
            
            pbar.update(20)
            pbar.set_description("📄 Reading mapping file")
            time.sleep(0.3)
            
            pbar.update(15)
            pbar.set_description("🗃️ Preparing file to load in database")
            
            dbt = dbtRunner()
            stdout_capture = StringIO()
            stderr_capture = StringIO()
            
            start_time = time.time()
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                result = dbt.invoke(seed_args)
            execution_time = time.time() - start_time
            
            current_progress = 45
            
            if execution_time < 1:
                pbar.update(55)
                pbar.set_description("✅ Updated mapping file")
            else:
                seed_steps = [
                    ("💾 Loading data", 25),
                    ("✅ Creating table", 20),
                    ("🎯 Finalizing", 10)
                ]
                
                step_time = execution_time / len(seed_steps)
                
                for step_desc, step_progress in seed_steps:
                    pbar.set_description(step_desc)
                    time.sleep(step_time * 0.7)
                    current_progress += step_progress
                    pbar.update(step_progress)
            
            if pbar.n < 100:
                pbar.update(100 - pbar.n)
            
            if result.success:
                pbar.set_description("✅ Updated mapping file")
            else:
                pbar.set_description("❌ Failed to update mapping file")

        return result

    def connection_check(self, dbname, schemaname, tablename):
        """Run dbt debug to check connection with actual progress tracking"""
        vars_str = self._build_vars_string(dbname, schemaname, tablename)
        debug_args = [
            "debug",
            "--project-dir", project_dir,
            "--profiles-dir", self.profiles_dir,
            "--vars", vars_str
        ]

        dbt = dbtRunner()
        stdout_capture = StringIO()
        stderr_capture = StringIO()

        with tqdm(
            total=100,
            desc="Establishing Connection",
            colour="green",
            bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(self.bar_width) + '}|'
        ) as pbar:
            
            pbar.update(10)
            pbar.set_description("🔧 Initializing dbt")
            time.sleep(0.5)
            
            pbar.update(10)
            pbar.set_description("📁 Loading profiles")
            time.sleep(0.5)
            
            pbar.update(10)
            pbar.set_description("🔌 Testing connection")
            
            start_time = time.time()
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                result = dbt.invoke(debug_args)
            execution_time = time.time() - start_time
            
            if execution_time < 2:
                pbar.update(70)
            else:
                connection_steps = [
                    ("🔍 Checking credentials", 15),
                    ("📡 Connecting to database", 25),
                    ("✅ Verifying permissions", 20),
                    ("🎯 Final validation", 10)
                ]
                
                step_time = execution_time / len(connection_steps)
                current_progress = 30
                
                for step_desc, step_progress in connection_steps:
                    pbar.set_description(step_desc)
                    time.sleep(step_time * 0.8)
                    current_progress += step_progress
                    pbar.update(step_progress)
            
            if pbar.n < 100:
                pbar.update(100 - pbar.n)
            
            if result.success:
                pbar.set_description("✅ Connection Established")
            else:
                pbar.set_description("❌ Connection Failed")

        return result

    def run_dbt(self, dbname, schemaname, tablename):
        """Run all dbt models with detailed progress tracking"""
        vars_str = self._build_vars_string(dbname, schemaname, tablename)
        run_args = [
            "run",
            "--project-dir", project_dir,
            "--profiles-dir", self.profiles_dir,
            "--vars", vars_str
        ]
        
        model_count = self.get_dbt_models_count()
        
        with tqdm(
            total=model_count, 
            desc="Running dbt models", 
            colour="green", 
            bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(self.bar_width) + '}| {n_fmt}/{total_fmt} models'
        ) as pbar:
            
            dbt = dbtRunner()
            stdout_capture = StringIO()
            stderr_capture = StringIO()
            
            completed_models = set()
            
            def track_progress_from_output():
                output = stdout_capture.getvalue()
                completion_patterns = [
                    r"\d+\s+of\s+\d+\s+OK\s+created.*model\s+([^\s]+)",
                    r"OK\s+created.*model\s+([^\s]+).*\[OK",
                    r"Completed\s+model\s+([^\s]+).*SUCCESS",
                ]
                
                for pattern in completion_patterns:
                    matches = re.findall(pattern, output)
                    for match in matches:
                        if match and match not in completed_models:
                            completed_models.add(match)
                            if pbar.n < model_count:
                                pbar.update(1)
                                model_name_short = match.split('.')[-1] if '.' in match else match
                                pbar.set_description(f"🔄 Running: {model_name_short}")
            
            def run_dbt_command():
                with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                    return dbt.invoke(run_args)
            
            result_container = []
            dbt_thread = threading.Thread(
                target=lambda: result_container.append(run_dbt_command())
            )
            dbt_thread.daemon = True
            dbt_thread.start()
            
            last_output_size = 0
            while dbt_thread.is_alive():
                current_output_size = stdout_capture.tell()
                if current_output_size > last_output_size:
                    track_progress_from_output()
                    last_output_size = current_output_size
                time.sleep(0.5)
            
            result = result_container[0] if result_container else None
            track_progress_from_output()
            
            if result and result.success and pbar.n < model_count:
                pbar.update(model_count - pbar.n)
            
            if result and result.success:
                pbar.set_description(f"✅ All {model_count} models executed successfully")
            else:
                completed_count = pbar.n
                pbar.set_description(f"❌ Execution failed ({completed_count}/{model_count} models completed)")
        
        return result

    def run_pre_run_setup(self, dbname, schemaname, tablename):
        """Run the pre_run_setup macro with progress tracking"""
        args_dict = {
            'db_name': dbname,
            'schema_name': schemaname,
            'table_name': tablename
        }
        vars_str = self._build_vars_string(dbname, schemaname, tablename)
        args_str = json.dumps(args_dict)
        macro_args = [
            "run-operation",
            "pre_run_setup",
            "--project-dir", project_dir,
            "--profiles-dir", self.profiles_dir,
            "--vars", vars_str,
            "--args", args_str
        ]
        
        with tqdm(
            total=100,
            desc="Running Pre setup Macro",
            colour="green",
            bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(self.bar_width) + '}|'
        ) as pbar:
            
            dbt = dbtRunner()
            stdout_capture = StringIO()
            stderr_capture = StringIO()
            
            pbar.update(15)
            pbar.set_description("🔧 Initializing macro")
            time.sleep(0.3)
            
            pbar.update(20)
            pbar.set_description("⚙️ Processing parameters")
            time.sleep(0.3)
            
            pbar.update(15)
            pbar.set_description("🗄️ Setting up database")
            
            start_time = time.time()
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                result = dbt.invoke(macro_args)
            execution_time = time.time() - start_time
            
            current_progress = 50
            
            if execution_time < 1:
                pbar.update(50)
            else:
                setup_steps = [
                    ("🔄 Creating temporary structures", 20),
                    ("📋 Configuring environment", 15),
                    ("✅ Setup complete", 15)
                ]
                
                step_time = execution_time / len(setup_steps)
                
                for step_desc, step_progress in setup_steps:
                    pbar.set_description(step_desc)
                    time.sleep(step_time * 0.7)
                    current_progress += step_progress
                    pbar.update(step_progress)
            
            if pbar.n < 100:
                pbar.update(100 - pbar.n)
            
            if result.success:
                pbar.set_description("✅ Pre-run setup completed")
            else:
                pbar.set_description("❌ Pre-run setup failed")
        
        return result

    def build_dbt_compile_args(self, dbname, schemaname, tablename):
        """Build arguments for dbt compile"""
        vars_str = self._build_vars_string(dbname, schemaname, tablename)
        return [
            "compile",
            "--project-dir", project_dir,
            "--profiles-dir", self.profiles_dir,
            "--vars", vars_str
        ]

    def run_dbt_args(self, cli_args, dbname, schemaname, tablename):
        """Run dbt with given arguments, with actual compilation progress tracking."""
        vars_str = self._build_vars_string(dbname, schemaname, tablename)
        cli_args += ["--vars", vars_str]

        is_compile = "compile" in cli_args
        
        if is_compile:
            model_count = self.get_dbt_models_count()
            with tqdm(
                total=model_count, 
                desc="Compiling dbt models",
                colour="green", 
                bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(self.bar_width) + '}| {n_fmt}/{total_fmt} models'
            ) as pbar:
                
                dbt = dbtRunner()
                stdout_capture = StringIO()
                stderr_capture = StringIO()
                
                compiled_models = set()
                
                def track_compile_progress_from_output():
                    output = stdout_capture.getvalue()
                    compile_patterns = [
                        r"Compiling\s+model\s+([^\s]+)",
                        r"Compiled\s+model\s+([^\s]+)",
                        r"Processing\s+model\s+([^\s]+)",
                        r"Found\s+(\d+)\s+models",
                    ]
                    
                    for pattern in compile_patterns:
                        matches = re.findall(pattern, output)
                        for match in matches:
                            if match and match not in compiled_models:
                                if match.isdigit():
                                    continue
                                compiled_models.add(match)
                                if pbar.n < model_count:
                                    pbar.update(1)
                                    model_name_short = match.split('.')[-1] if '.' in match else match
                                    pbar.set_description(f"📝 Compiling: {model_name_short}")
                
                def run_dbt_command():
                    with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                        return dbt.invoke(cli_args)
                
                result_container = []
                dbt_thread = threading.Thread(
                    target=lambda: result_container.append(run_dbt_command())
                )
                dbt_thread.daemon = True
                dbt_thread.start()
                
                last_output_size = 0
                while dbt_thread.is_alive():
                    current_output_size = stdout_capture.tell()
                    if current_output_size > last_output_size:
                        track_compile_progress_from_output()
                        last_output_size = current_output_size
                    time.sleep(0.3)
                
                result = result_container[0] if result_container else None
                track_compile_progress_from_output()
                
                if result and result.success and pbar.n < model_count:
                    pbar.update(model_count - pbar.n)
                
                if result and result.success:
                    pbar.set_description(f"✅ All {model_count} models compiled successfully")
                else:
                    compiled_count = pbar.n
                    pbar.set_description(f"❌ Compilation failed ({compiled_count}/{model_count} models compiled)")
            
            return result
            
        else:
            with tqdm(
                total=100,
                desc="Running dbt command",
                colour="green",
                bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(self.bar_width) + '}|'
            ) as pbar:
                
                dbt = dbtRunner()
                stdout_capture = StringIO()
                stderr_capture = StringIO()
                
                pbar.update(20)
                pbar.set_description("🔧 Initializing command")
                time.sleep(0.3)
                
                pbar.update(30)
                pbar.set_description("⚡ Executing command")
                
                start_time = time.time()
                with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                    result = dbt.invoke(cli_args)
                execution_time = time.time() - start_time
                
                current_progress = 50
                
                if execution_time < 1:
                    pbar.update(50)
                else:
                    command_steps = [
                        ("🔄 Processing request", 25),
                        ("✅ Command complete", 25)
                    ]
                    
                    step_time = execution_time / len(command_steps)
                    
                    for step_desc, step_progress in command_steps:
                        pbar.set_description(step_desc)
                        time.sleep(step_time * 0.7)
                        current_progress += step_progress
                        pbar.update(step_progress)
                
                if pbar.n < 100:
                    pbar.update(100 - pbar.n)
                
                if result.success:
                    pbar.set_description("✅ Command completed successfully")
                else:
                    pbar.set_description("❌ Command failed")
            
            return result