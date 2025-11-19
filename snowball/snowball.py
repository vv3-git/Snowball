# run_dbt.py
"""
    Main operating python file for generating formatted compiled version of snowball project via dbt
    and packaging it as per the requirements.
    It also generates pyspark notebooks from the compiled SQL files.
"""
""" Import necessary libraries """
import os
import re
import sys
import json
import zipfile
import shutil
import subprocess
import msvcrt
import time
from .config import *
from git import Repo
from pathlib import Path
from datetime import datetime
from dbt.cli.main import dbtRunner
import nbformat as nbf
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell
from tqdm import tqdm
import threading
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO

compiled_dir  = os.path.join(project_dir, "target", "compiled")
dbt_seed_dir  = os.path.join(project_dir, "seeds")
profiles_dir  = str(Path.home() / "Downloads/snowball_dbt")
notebooks_dir = os.path.join(output_dir, "notebooks")

term_width = shutil.get_terminal_size().columns
bar_width = term_width // 4
############  Formatting Functions ##############

def welcome_message():
    print("\n")
    message = "Welcome to Snowball Product!"
    width = len(message) + 8  # padding for stars
    border = "*" * width

    # Prepare the lines to print
    line1 = border
    line2 = "*" + message.center(width - 2) + "*"
    line3 = border

    # Get terminal width
    term_width = shutil.get_terminal_size().columns

    # Center the output lines relative to terminal width
    print(line1.center(term_width))
    print(line2.center(term_width))
    print(line3.center(term_width))

def rotating_slash_after(text, duration_sec=5, passed=1):
    print(text, end=" ", flush=True)
    spinner = ['|', '/', '-', '\\']
    end_time = time.time() + duration_sec
    i = 0
    while time.time() < end_time:
        sys.stdout.write('\r' + text + " " + spinner[i % len(spinner)])
        sys.stdout.flush()
        time.sleep(0.1)
        i += 1
    green_tick = "\u2714"     # Unicode check mark
    green_color = "\033[92m"  # ANSI green
    red_cross = "\u274C"
    red_color = "\033[91m"
    reset_color = "\033[0m"   # Reset color
    if(passed == 1):
        print_msg = f"{text} {green_color}Done {green_tick}{reset_color}\n"
    else:
        print_msg = f"{text} {red_color}Failed {red_cross}{reset_color}\n"
    sys.stdout.write('\r' + print_msg)
    sys.stdout.flush()

def blinking_dots_input(base_text="Press Enter to continue"):
    dots = ['', '.', '..', '...']
    i = 0
    print(base_text, end='', flush=True)

    while True:
        print('\r' + base_text + dots[i % len(dots)] + '   ', end='', flush=True)
        time.sleep(0.5)
        i += 1

        # Non-blocking key check for Enter press
        if msvcrt.kbhit():
            key = msvcrt.getwch()
            if key == '\r':  # Enter key on Windows
                break

def initial_set_up():
    text = "Setting up initial requirements"
    width = len(text) + 8  # padding for stars
    border = "*" * width * 2

    # Prepare the lines to print
    line1 = border
    line2 = " " + text.center(width - 2)
    line3 = ("*" * (len(line2) // 3)).center(width - 2)
    line4 = "1. Collecting latest repo from Git "
    line5 = f"2. A Column mapping file has been downloaded to {Path.home()}\Downloads\column_mapping.csv with dummy data for reference. "
    line6 = "3. Update the levels and dimensions in the downloaded column_mapping file as per your revenue data and save it in Downloads"
    line7 = "4. Similar to column mapping, Locate and update your data platform credentials also in the profiles.yml(which is like .env file) from Downloads"
    # line7 = "4. Create a folder .dbt in the root directory and create a profiles.yml file"
    # line8 = "5. Update profiles.yml with your database credentials - Please refer Readme for more details [https://github.com/jmangroup/snowball_dbt#]"
    line9 = "If you are done with your updates, Press Enter to continue "

    # Get terminal width
    term_width = shutil.get_terminal_size().columns

    # Center the output lines relative to terminal width
    print(line1)
    print(line2)
    print(line3)
    rotating_slash_after(line4, 5)
    rotating_slash_after(line5, 1)
    print(line6)
    print(line7)
    # print(line8)
    blinking_dots_input(line9)
    print("\n")
    print(line1)
    print("\n")

def show_progress(desc, duration=None, steps=None):
    """Show a progress bar for a given operation"""
    if duration:
        # Time-based progress bar
        with tqdm(total=100, desc=desc, colour="green", bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(bar_width) + '}| {elapsed}') as pbar:
            step_time = duration / 100
            for i in range(100):
                time.sleep(step_time)
                pbar.update(1)
    elif steps:
        # Step-based progress bar
        pbar = tqdm(total=steps, desc=desc, bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(bar_width) + '}| {n_fmt}/{total_fmt}')
        return pbar
    else:
        # Indeterminate progress bar
        return tqdm(desc=desc, bar_format='{desc}  Processing...')

def cleanup_previous_run(cleaning_list):
    """Clean up previous compiled files and notebooks"""
    for dir_path in cleaning_list:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)

def get_dbt_models_count():
    """Count the number of dbt models in the project"""
    models_dir = os.path.join(project_dir, "models")
    model_count = 0
    if os.path.exists(models_dir):
        for root, _, files in os.walk(models_dir):
            model_count += len([f for f in files if f.endswith('.sql')])
    return max(model_count, 1)  # At least 1 to avoid division by zero

def run_dbt_deps(dbname, schemaname, tablename):
    """Run dbt deps to install dependencies with realistic progress tracking"""
    vars_dict = {
        'my_database': dbname,
        'my_schema': schemaname,
        'my_table': tablename
    }
    vars_str = json.dumps(vars_dict)
    deps_args = [
        "deps",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
        "--vars", vars_str
    ]
    
    with tqdm(
        total=100,
        desc="Installing dependencies",
        colour="green",
        bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(bar_width) + '}|'
    ) as pbar:
        
        # Phase 1: Initial setup and package discovery
        pbar.update(15)
        pbar.set_description("🔍 Discovering packages")
        time.sleep(0.5)
        
        # Phase 2: Reading dependencies file
        pbar.update(15)
        pbar.set_description("📖 Reading packages.yml")
        time.sleep(0.5)
        
        # Phase 3: Downloading packages
        pbar.update(20)
        pbar.set_description("📥 Downloading packages")
        
        # Actually run dbt deps
        dbt = dbtRunner()
        stdout_capture = StringIO()
        stderr_capture = StringIO()
        
        start_time = time.time()
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            result = dbt.invoke(deps_args)
        execution_time = time.time() - start_time
        
        # Update progress based on actual execution time
        current_progress = 50
        
        if execution_time < 1:
            # Very fast installation
            pbar.update(50)
            pbar.set_description("✅ Dependencies installed")
        else:
            # Simulate progressive installation steps
            installation_steps = [
                ("🔗 Resolving dependencies", 15),
                ("📦 Installing packages", 20),
                ("✅ Verifying installations", 15)
            ]
            
            step_time = execution_time / len(installation_steps)
            
            for step_desc, step_progress in installation_steps:
                pbar.set_description(step_desc)
                time.sleep(step_time * 0.7)  # Use 70% of step time for visual feedback
                current_progress += step_progress
                pbar.update(step_progress)
        
        # Ensure we reach 100%
        if pbar.n < 100:
            pbar.update(100 - pbar.n)
        
        if result.success:
            pbar.set_description("✅ Dependencies installed")
        else:
            pbar.set_description("❌ Dependencies failed")
    
    return result

def run_dbt_seed(dbname, schemaname, tablename):
    """Run dbt seed to update the user mapping file with realistic progress tracking"""
    vars_dict = {
        'my_database': dbname,
        'my_schema': schemaname,
        'my_table': tablename
    }
    vars_str = json.dumps(vars_dict)
    seed_args = [
        "seed",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
        "--vars", vars_str
    ]
    
    with tqdm(
        total=100,
        desc="Updating mapping file",
        colour="green",
        bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(bar_width) + '}|'
    ) as pbar:
        
        # Phase 1: Initial setup
        pbar.update(10)
        pbar.set_description("🔧 Initializing seed")
        time.sleep(0.3)
        
        # Phase 2: Reading seed file
        pbar.update(20)
        pbar.set_description("📄 Reading mapping file")
        time.sleep(0.3)
        
        # Phase 3: Database preparation
        pbar.update(15)
        pbar.set_description("🗃️ Preparing file to load in database")
        
        # Actually run dbt seed
        dbt = dbtRunner()
        stdout_capture = StringIO()
        stderr_capture = StringIO()
        
        start_time = time.time()
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            result = dbt.invoke(seed_args)
        execution_time = time.time() - start_time
        
        # Update progress based on actual execution time
        current_progress = 45
        
        if execution_time < 1:
            # Very fast seed operation
            pbar.update(55)
            pbar.set_description("✅ Updated mapping file")
        else:
            # Simulate progressive seed steps
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
        
        # Ensure we reach 100%
        if pbar.n < 100:
            pbar.update(100 - pbar.n)
        
        if result.success:
            pbar.set_description("✅ Updated mapping file")
        else:
            pbar.set_description("❌ Failed to update mapping file")

    return result

def connection_check(dbname, schemaname, tablename):
    """Run dbt debug to check connection with actual progress tracking"""
    vars_dict = {
        'my_database': dbname,
        'my_schema': schemaname,
        'my_table': tablename
    }
    vars_str = json.dumps(vars_dict)
    debug_args = [
        "debug",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
        "--vars", vars_str
    ]

    dbt = dbtRunner()
    stdout_capture = StringIO()
    stderr_capture = StringIO()

    with tqdm(
        total=100,
        desc="Establishing Connection",
        colour="green",
        bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(bar_width) + '}|'
    ) as pbar:
        
        # Phase 1: Initial setup (10%)
        pbar.update(10)
        pbar.set_description("🔧 Initializing dbt")
        time.sleep(0.5)
        
        # Phase 2: Profile loading (20%)
        pbar.update(10)
        pbar.set_description("📁 Loading profiles")
        time.sleep(0.5)
        
        # Phase 3: Connection testing (30%)
        pbar.update(10)
        pbar.set_description("🔌 Testing connection")
        
        # Actually run the debug command and capture real-time progress
        start_time = time.time()
        # print(vars_dict)
        # print(f"[DEBUG] Running dbt debug with args: {debug_args}")
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            result = dbt.invoke(debug_args)
        execution_time = time.time() - start_time
        
        # Based on actual execution time, update progress more realistically
        if execution_time < 2:
            # Fast connection - jump to completion
            pbar.update(70)
        else:
            # Simulate progressive connection steps based on actual time
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
                time.sleep(step_time * 0.8)  # Use 80% of step time for visual feedback
                current_progress += step_progress
                pbar.update(step_progress)
        
        # Ensure we reach 100%
        if pbar.n < 100:
            pbar.update(100 - pbar.n)
        
        if result.success:
            pbar.set_description("✅ Connection Established")
        else:
            pbar.set_description("❌ Connection Failed")

    return result

# def run_dbt(dbname, schemaname, tablename):
#     """Run all dbt models with detailed progress tracking"""
#     vars_dict = {
#         'my_database': dbname,
#         'my_schema': schemaname,
#         'my_table': tablename
#     }
#     vars_str = json.dumps(vars_dict)
#     run_args = [
#         "run",
#         "--project-dir", project_dir,
#         "--profiles-dir", profiles_dir,
#         "--vars", vars_str
#     ]
    
#     # Get estimated model count for progress tracking
#     model_count = get_dbt_models_count()
    
#     with tqdm(
#         total=model_count, 
#         desc="Running dbt models", 
#         colour="green", 
#         bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(bar_width) + '}| {n_fmt}/{total_fmt} models'
#     ) as pbar:
        
#         dbt = dbtRunner()
#         stdout_capture = StringIO()
#         stderr_capture = StringIO()
        
#         # Track completed models
#         completed_models = set()
        
#         def track_progress_from_output():
#             """Parse stdout to track model completion progress"""
#             output = stdout_capture.getvalue()
#             # Look for model completion patterns in dbt output
#             completion_patterns = [
#                 r"\d+\s+of\s+\d+\s+OK\s+created.*model\s+([^\s]+)",  # "1 of 18 OK created sql table model core.revenue"
#                 r"OK\s+created.*model\s+([^\s]+).*\[OK",  # "OK created sql table model core.revenue [OK in 6.06s]"
#                 r"Completed\s+model\s+([^\s]+).*SUCCESS",  # Alternative pattern
#             ]
            
#             for pattern in completion_patterns:
#                 matches = re.findall(pattern, output)
#                 for match in matches:
#                     if match and match not in completed_models:
#                         completed_models.add(match)
#                         if pbar.n < model_count:
#                             pbar.update(1)
#                             model_name_short = match.split('.')[-1] if '.' in match else match
#                             pbar.set_description(f"🔄 Running: {model_name_short}")
        
#         # Run dbt with output capture
#         start_time = time.time()
        
#         # Use threading to monitor progress while dbt runs
#         def run_dbt_command():
#             with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
#                 return dbt.invoke(run_args)
        
#         # Run dbt in a separate thread
#         result_container = []
#         dbt_thread = threading.Thread(
#             target=lambda: result_container.append(run_dbt_command())
#         )
#         dbt_thread.daemon = True
#         dbt_thread.start()
        
#         # Monitor progress while dbt is running
#         last_output_size = 0
#         while dbt_thread.is_alive():
#             # Check for new output and track progress
#             current_output_size = stdout_capture.tell()
#             if current_output_size > last_output_size:
#                 track_progress_from_output()
#                 last_output_size = current_output_size
#             time.sleep(0.5)  # Check every 500ms
        
#         # Get the final result
#         result = result_container[0] if result_container else None
        
#         # Final progress check with all output
#         track_progress_from_output()
        
#         # Ensure progress bar reaches 100% if successful
#         if result and result.success and pbar.n < model_count:
#             pbar.update(model_count - pbar.n)
        
#         if result and result.success:
#             pbar.set_description(f"✅ All {model_count} models executed successfully")
#         else:
#             completed_count = pbar.n
#             pbar.set_description(f"❌ Execution failed ({completed_count}/{model_count} models completed)")
    
#     return result


def run_dbt(dbname, schemaname, tablename):
    """Run all dbt models with detailed progress tracking and logging."""
    vars_dict = {
        'my_database': dbname,
        'my_schema': schemaname,
        'my_table': tablename
    }
    vars_str = json.dumps(vars_dict)

    run_args = [
        "run",
        "--project-dir", project_dir,
        "--profiles-dir", os.path.dirname(profiles_dir),
        "--vars", vars_str
    ]

    model_count = get_dbt_models_count()

    with tqdm(
        total=model_count,
        desc="Running dbt models",
        colour="green",
        bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(bar_width) + '}| {n_fmt}/{total_fmt} models'
    ) as pbar:

        dbt = dbtRunner()
        stdout_capture = StringIO()
        stderr_capture = StringIO()
        completed_models = set()

        def track_progress_from_output():
            """Parse stdout to track model completion progress."""
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

        start_time = time.time()
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

    # --- ✅ NEW: Save full dbt run logs for debugging ---
    log_path = os.path.join(output_dir, "dbt_run_log.txt")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("=== DBT RUN STDOUT ===\n")
        f.write(stdout_capture.getvalue())
        f.write("\n\n=== DBT RUN STDERR ===\n")
        f.write(stderr_capture.getvalue())

    print(f"[DEBUG] Full dbt run logs saved to: {log_path}")

    return result


def run_pre_run_setup(dbname, schemaname, tablename):
    """Run the pre_run_setup macro with progress tracking"""
    args_dict = {
        'db_name': dbname,
        'schema_name': schemaname,
        'table_name': tablename
    }
    vars_dict = {
        'my_database': dbname,
        'my_schema': schemaname,
        'my_table': tablename
    }
    vars_str = json.dumps(vars_dict)
    args_str = json.dumps(args_dict)
    macro_args = [
        "run-operation",
        "pre_run_setup",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
        "--vars", vars_str,
        "--args", args_str
    ]
    
    with tqdm(
        total=100,
        desc="Running Pre setup Macro",
        colour="green",
        bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(bar_width) + '}|'
    ) as pbar:
        
        dbt = dbtRunner()
        stdout_capture = StringIO()
        stderr_capture = StringIO()
        
        # Phase 1: Initialization
        pbar.update(15)
        pbar.set_description("🔧 Initializing macro")
        time.sleep(0.3)
        
        # Phase 2: Parameter processing
        pbar.update(20)
        pbar.set_description("⚙️ Processing parameters")
        time.sleep(0.3)
        
        # Phase 3: Database preparation
        pbar.update(15)
        pbar.set_description("🗄️ Setting up database")
        
        # Actually run the macro
        start_time = time.time()
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            result = dbt.invoke(macro_args)
        execution_time = time.time() - start_time
        
        # Update progress based on actual execution time
        current_progress = 50
        
        if execution_time < 1:
            # Very fast execution
            pbar.update(50)
        else:
            # Simulate progressive setup steps
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
        
        # Ensure we reach 100%
        if pbar.n < 100:
            pbar.update(100 - pbar.n)
        
        if result.success:
            pbar.set_description("✅ Pre-run setup completed")
        else:
            pbar.set_description("❌ Pre-run setup failed")
    
    return result

def build_dbt_compile_args(dbname, schemaname, tablename):
    """Build arguments for dbt compile"""
    vars_dict = {
        'my_database': dbname,
        'my_schema': schemaname,
        'my_table': tablename
    }
    vars_str = json.dumps(vars_dict)
    return [
        "compile",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
        "--vars", vars_str
    ]

def run_dbt_args(cli_args, dbname, schemaname, tablename):
    """Run dbt with given arguments, with actual compilation progress tracking."""
    vars_dict = {
        'my_database': dbname,
        'my_schema': schemaname,
        'my_table': tablename
    }
    vars_str = json.dumps(vars_dict)
    cli_args += ["--vars", vars_str]

    # Check if this is a compile operation for enhanced progress tracking
    is_compile = "compile" in cli_args
    
    if is_compile:
        model_count = get_dbt_models_count()
        with tqdm(
            total=model_count, 
            desc="Compiling dbt models",
            colour="green", 
            bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(bar_width) + '}| {n_fmt}/{total_fmt} models'
        ) as pbar:
            
            dbt = dbtRunner()
            stdout_capture = StringIO()
            stderr_capture = StringIO()
            
            # Track compiled models
            compiled_models = set()
            
            def track_compile_progress_from_output():
                """Parse stdout to track model compilation progress"""
                output = stdout_capture.getvalue()
                # Look for compilation patterns in dbt output
                compile_patterns = [
                    r"Compiling\s+model\s+([^\s]+)",  # "Compiling model core.revenue"
                    r"Compiled\s+model\s+([^\s]+)",   # "Compiled model core.revenue"
                    r"Processing\s+model\s+([^\s]+)", # "Processing model core.revenue"
                    r"Found\s+(\d+)\s+models",        # "Found 18 models" - for initial count
                ]
                
                for pattern in compile_patterns:
                    matches = re.findall(pattern, output)
                    for match in matches:
                        if match and match not in compiled_models:
                            # If it's a number from "Found X models", skip
                            if match.isdigit():
                                continue
                            compiled_models.add(match)
                            if pbar.n < model_count:
                                pbar.update(1)
                                model_name_short = match.split('.')[-1] if '.' in match else match
                                pbar.set_description(f"📝 Compiling: {model_name_short}")
            
            # Run dbt compile with output capture
            start_time = time.time()
            
            # Use threading to monitor progress while dbt runs
            def run_dbt_command():
                with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                    return dbt.invoke(cli_args)
            
            # Run dbt in a separate thread
            result_container = []
            dbt_thread = threading.Thread(
                target=lambda: result_container.append(run_dbt_command())
            )
            dbt_thread.daemon = True
            dbt_thread.start()
            
            # Monitor progress while dbt is running
            last_output_size = 0
            while dbt_thread.is_alive():
                # Check for new output and track progress
                current_output_size = stdout_capture.tell()
                if current_output_size > last_output_size:
                    track_compile_progress_from_output()
                    last_output_size = current_output_size
                time.sleep(0.3)  # Check every 300ms for compile (faster operation)
            
            # Get the final result
            result = result_container[0] if result_container else None
            
            # Final progress check with all output
            track_compile_progress_from_output()
            
            # Ensure progress bar reaches 100% if successful
            if result and result.success and pbar.n < model_count:
                pbar.update(model_count - pbar.n)
            
            if result and result.success:
                pbar.set_description(f"✅ All {model_count} models compiled successfully")
            else:
                compiled_count = pbar.n
                pbar.set_description(f"❌ Compilation failed ({compiled_count}/{model_count} models compiled)")
        
        return result
        
    else:
        # For non-compile operations, use simple progress
        with tqdm(
            total=100,
            desc="Running dbt command",
            colour="green",
            bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(bar_width) + '}|'
        ) as pbar:
            
            dbt = dbtRunner()
            stdout_capture = StringIO()
            stderr_capture = StringIO()
            
            # Initial setup
            pbar.update(20)
            pbar.set_description("🔧 Initializing command")
            time.sleep(0.3)
            
            # Command execution
            pbar.update(30)
            pbar.set_description("⚡ Executing command")
            
            # Actually run the command with output suppressed
            start_time = time.time()
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                result = dbt.invoke(cli_args)
            execution_time = time.time() - start_time
            
            # Update progress based on execution time
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
            
            # Ensure we reach 100%
            if pbar.n < 100:
                pbar.update(100 - pbar.n)
            
            if result.success:
                pbar.set_description("✅ Command completed successfully")
            else:
                pbar.set_description("❌ Command failed")
        
        return result

def zip_directory(source_dir, zip_path):
    """Zip the contents of an entire directory"""
    # Count total files first
    total_files = 0
    for root, _, files in os.walk(source_dir):
        total_files += len(files)
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        with tqdm(total=total_files, desc="Creating archive", colour="green", bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(bar_width) + '}|') as pbar:
            for root, _, files in os.walk(source_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, source_dir)
                    zipf.write(file_path, arcname)
                    pbar.update(1)

def run_sqlfluff_on_directory(directory_path, project_root):
    """
    Run SQLFluff fix on all SQL files in a directory.
    """
    try:
        # Collect all SQL files
        sql_files = []
        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.endswith('.sql'):
                    sql_files.append(os.path.join(root, file))
        
        if not sql_files:
            return True
        
        success_count = 0
        with tqdm(total=len(sql_files), desc="Applying SQLFluff", colour="green",bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(bar_width) + '}|') as pbar:
            for sql_file in sql_files:
                try:
                    # Run SQLFluff on individual file
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

def apply_sqlfluff_to_compiled(project_root):
    """
    Apply SQLFluff to all compiled SQL files before packaging.
    Run SQLFluff on the entire compiled models directory at once for efficiency.
    """
    # check SQLFluff is availability
    try:
        subprocess.run(["sqlfluff", "--version"], check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        with tqdm(desc="❌ SQLFluff not available", bar_format='{desc}') as pbar:
            time.sleep(1)
        return False
    
    # Run SQLFluff on the compiled models directory
    return run_sqlfluff_on_directory(compiled_dir, project_root)

def generate_notebooks():
    """Generate Jupyter notebooks from compiled SQL by model folder"""
    try:
        os.makedirs(notebooks_dir, exist_ok=True)

        model_folders = set()
        for root, _, files in os.walk(compiled_dir):
            for file in files:
                if file.endswith('.sql'):
                    rel_path = os.path.relpath(root, compiled_dir)
                    if rel_path != '.':
                        folder_name = rel_path.split(os.sep)[-1]
                        if folder_name != "models":
                            model_folders.add(folder_name)

        with tqdm(total=len(model_folders), desc="📓 Generating notebooks", bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(bar_width) + '}|') as pbar:
            for folder in model_folders:
                if folder == "models":
                    continue
                    
                notebook_path = os.path.join(notebooks_dir, f"{folder}_nb.ipynb")
                nb = new_notebook()
                
                folder_name = folder.upper().split('_')[-1]
                nb.cells.append(new_markdown_cell(
                    "## SNOWBALL Spark SQL version\n"
                    f"#### **Notebook to create {folder_name} layer**\n"
                    f"##### **Creating {folder_name} schema to create required {folder_name} tables**\n"
                ))
                nb.cells.append(new_code_cell(f"%%sql\nCREATE SCHEMA IF NOT EXISTS {folder.split('_')[-1]};"))

                if folder == 'tests':
                    folder_path = os.path.join(compiled_dir, 'Snowball_dbt', folder)
                else:            
                    folder_path = os.path.join(compiled_dir, 'Snowball_dbt', 'models', folder)

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

def copy_seed_file(seed_path, target_dir, dbname, schemaname, tablename):
    """
    Copy (or replace) the column_mapping.csv file to the dbt seeds directory
    and run dbt seed to register it.
    """
    try:
        shutil.copy(seed_path, target_dir)

        # Run dbt seed to update
        run_dbt_seed(dbname, schemaname, tablename)
        return True

    except FileNotFoundError:
        with tqdm(desc="❌ Mapping file not found", bar_format='{desc}') as pbar:
            time.sleep(1)
        return False
    except Exception as e:
        with tqdm(desc=f"❌ Failed to process mapping file: {e}", bar_format='{desc}') as pbar:
            time.sleep(1)
        return False

def update_profile(profile_src_path, profiles_dir):
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

def transform_compiled_sql(sql_file_path):
    """Post-process a compiled SQL file to wrap in stored procedure format."""
    try:
        with open(sql_file_path, "r", encoding="utf-8") as f:
            sql_code = f.read()

        # Extract folder inside models and model name
        rel_path = os.path.relpath(sql_file_path, compiled_dir)
        parts = rel_path.split(os.sep)

        # Find index of "models" in path
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

        # === 1. Add schema + procedure header ===
        proc_header = (
            f"CREATE OR ALTER PROCEDURE {model_folder_name}.sp_{model_name}\n"
            f"AS\nBEGIN\n    SET NOCOUNT ON;\n\n"
            f"    BEGIN\n        DROP TABLE IF EXISTS {model_folder_name}.{model_name}; \n    END;\n\n"
        )

        # === 2. Add INTO clause before last FROM ===
        match = list(re.finditer(r"\bFROM\b", sql_code, re.IGNORECASE))
        if match:
            last_from = match[-1]
            insert_pos = last_from.start()
            sql_code = sql_code[:insert_pos] + f"INTO {model_folder_name}.{model_name}\n" + sql_code[insert_pos:]

        # === 3. Add END ===
        sql_code = proc_header + sql_code.strip() + "\nEND;"

        # Overwrite the file
        with open(sql_file_path, "w", encoding="utf-8") as f:
            f.write(sql_code)

    except Exception:
        pass

def process_compiled_sql_files():
    """Walk through compiled models directory and transform all SQL files."""
    # Count SQL files first
    sql_files = []
    for root, _, files in os.walk(compiled_dir):
        for file in files:
            if file.endswith(".sql"):
                sql_files.append(os.path.join(root, file))
    
    with tqdm(total=len(sql_files), desc="Transforming SQL files", colour="green", bar_format='{desc}  {percentage:3.0f}%|{bar:' + str(bar_width) + '}|') as pbar:
        for sql_file in sql_files:
            transform_compiled_sql(sql_file)
            pbar.update(1)

def remove_readonly_files(func, path, _):
    """Error handler for removing read-only files on Windows"""
    import stat
    os.chmod(path, stat.S_IWRITE)
    func(path)

def copy_snowball_dbt(source_path: str) -> str:
    """
    Get the driving snowball dbt folder / code into downloads
    
    Args:
        source_path (str): Path to the source directory to copy.
        
    Returns:
        str: Path to the column_mapping.csv file
    """
    source_path = os.path.normpath(os.path.abspath(source_path))

    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source directory not found: {source_path}")

    repo_name = os.path.basename(source_path)
    downloads_dir = Path.home() / "Downloads"
    temp_clone_location = downloads_dir / f"{repo_name}_temp_snowball_driver"
    final_clone_location = downloads_dir / repo_name

    if final_clone_location.exists():
        shutil.rmtree(final_clone_location, onerror=remove_readonly_files)
    if temp_clone_location.exists():
        shutil.rmtree(temp_clone_location, onerror=remove_readonly_files)

    shutil.copytree(source_path, temp_clone_location, dirs_exist_ok=True)
    temp_clone_location.rename(final_clone_location)

    mapping_file = final_clone_location / "seeds" / "column_mapping.csv"
    if not mapping_file.exists():
        raise FileNotFoundError(f"Column mapping file not found at: {mapping_file}")

    return str(mapping_file)

def copy_csv_to_downloads(src_csv_path: str) -> str:
    """
    Copy a CSV file from src_csv_path to the Downloads folder of the current user.
    
    Args:
        src_csv_path (str): The source path of the CSV file to copy.
        
    Returns:
        str: The full path to the copied file in the Downloads folder.
    """
    # Get the user's Downloads folder path dynamically
    downloads_dir = Path.home() / "Downloads"
    
    # Ensure Downloads folder exists (usually it does)
    downloads_dir.mkdir(parents=True, exist_ok=True)
    
    src_path = Path(src_csv_path)
    if not src_path.is_file():
        raise FileNotFoundError(f"Source CSV file not found: {src_csv_path}")
    
    # Destination path keeps the same filename
    dest_path = downloads_dir / src_path.name
    shutil.copy(src_path, dest_path)
    
    try:
        os.chmod(dest_path, 0o666)  # read/write for owner, read for group/others
    except Exception as e:
        print(f"Warning: Could not set permissions on {dest_path}: {e}")
    
    return str(dest_path)

def copy_profiles_to_downloads(profiles_src_path: str) -> str:
    """
    Copy profiles.yml from the DBT project to the user's Downloads folder
    (for the user to update credentials), similar to column_mapping.
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

def update_revenue_model_with_table_name(output_dir, table_name):
    """
    Replace occurrences of '.snowball_revenue' with the provided table_name
    in all compiled revenue.sql model files across all versions/platforms.
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

def main():
    welcome_message()

    # Clean up previous runs
    # cleanup_previous_run()

    # Clone the latest repo from Snowball dbt
    # Get the snowball_versions path from the package
    current_dir = os.path.dirname(os.path.abspath(__file__))
    snowball_versions_path = os.path.join(current_dir, "snowball_versions", "snowball_dbt")
    
    if not os.path.exists(snowball_versions_path):
        print(f"❌ Source directory not found: {snowball_versions_path}")
        print("Please ensure the snowball_versions folder is inside the snowball package directory")
        return

    try:
        mapping_file_path = copy_snowball_dbt(snowball_versions_path)
        copy_csv_to_downloads(mapping_file_path)

        # Fix: Look for profiles.yml in the original source location, not the copied location
        profiles_src_path = os.path.join(snowball_versions_path, "profiles.yml")
        
        # If not found in source, try the parent directory
        if not os.path.exists(profiles_src_path):
            profiles_src_path = os.path.join(os.path.dirname(snowball_versions_path), "profiles.yml")
            
        # If still not found, try the project root
        if not os.path.exists(profiles_src_path):
            project_root = os.path.dirname(current_dir)
            profiles_src_path = os.path.join(project_root, "profiles.yml")
            
        if not os.path.exists(profiles_src_path):
            print(f"❌ profiles.yml not found in any expected location")
            print(f"   Checked: {profiles_src_path}")
            return
            
        copy_profiles_to_downloads(profiles_src_path)
        
    except Exception as e:
        print(f"❌ Error setting up snowball: {e}")
        return
    
    initial_set_up()
    
    project_root = project_dir
    text = f"Checking Database Connection!"
    width = len(text) + 8  # padding for stars
    border = "*" * width * 2

    # Prepare the lines to print
    line1 = border
    line2 = " " + text.center(width - 2)
    line3 = ("*" * (len(line2) // 3)).center(width - 2)
    print(line1)
    print(line2)
    print(line3)
    tablename = input('Enter revenue table name : ')
    db_config = load_dbt_profile("Snowball_dbt", "dev")
    dbname = db_config.get("database")
    schemaname = db_config.get("schema")
    type = db_config.get("type")

    print(f"Revenue table: {dbname}.{schemaname}.{tablename}")
   
    update_profile(dbt_profiles_dir, profiles_dir)
    
    print("Available Database Platform")
    print("     1: Snowflake")
    print("     2: Databricks")
    print("     3: Fabric")
    print("     4: SQL database")
    print("     5: Redshift --In Progress")

    platform_dict = {
        1: "snowflake",
        2: "databricks",
        3: "fabric",
        4: "sqlserver",
        5: "redshift"
    }
    user_choice = int(input("\nSelect your Database Platform [1-5]: ").strip())
    while 1:
        try:
            if user_choice in [1, 2, 3, 4, 5]:
                break
            else: user_choice = int(input("❌ Invalid input. Please enter between [1-5]: ").strip())
        except ValueError:
            print("❌ Invalid input. Please enter between [1-5].")

    def checking():
        connection = connection_check(dbname,schemaname,tablename)
        if not connection.success or type != platform_dict.get(user_choice):
            print("\U0001F641 Connection Failed! \n")
            blinking_dots_input("Update Your Profiles.yml correctly and Press Enter to check the connection again!! ")
            checking()
        if connection.success:
            # rotating_slash_after(line4,8,1)
            print("\U0001F642 Connection Established Successfully! \n")
    checking()
    
    text = f"Database Platform | Snowball Version!"
    width = len(text) + 8  # padding for stars
    border = "*" * width

    # Prepare the lines to print
    line1 = border
    line2 = " " + text.center(width - 2)
    line3 = ("*" * (len(line2) // 3)).center(width - 2)
    print(line1)
    print(line2)
    print(line3)
    
    deps_result = run_dbt_deps(dbname, schemaname, tablename)
    if not deps_result.success:
        print("❌ dbt deps failed")
        return

    copy_seed_file(mapping_file, dbt_seed_dir, dbname, schemaname, tablename)
    print(line3)
    print("Available Snowball Version")
    print("     1: dbt")
    print("     2: sql")
    print("     3: Spark sql")
    print("     4: Redshift - N/A")

    ''' Final Ending Text applied in all version choices '''
    final_text = "  Thanks for using Snowball Product! Happy coding! \U0001F642  "
    # Get terminal width
    term_width = shutil.get_terminal_size().columns
    try:
        user_choice_version = int(input("\nSelect your Snowball Version: ").strip())
    except ValueError:
        print("❌ Invalid input. Please enter [1-4].")
        return
    
    print(f"{line1}\n")
    if user_choice_version == 1:
        if user_choice == 1:
            text = "Generating Snowflake adaptable dbt code "
        if user_choice == 2:
            text = "Generating Databricks adaptable dbt code ..."
        if user_choice == 3:
            text = "Generating Fabric adaptable dbt code ..."

        try:
            rotating_slash_after(text, 10)
            output_zip = os.path.join(output_dir, "snowball_dbt.zip")
            update_revenue_model_with_table_name(project_dir, tablename)
            zip_directory(project_dir, output_zip)
            print(f"snowball_dbt code is saved at: {output_zip}\n")
            print(final_text.center(term_width, '*'))
        except Exception as e:
            print(f"❌ Failed to zip dbt project: {e}")

    elif user_choice_version == 2:
        print("Generating SQL code...")
        
        try:
            output_zip = os.path.join(output_dir, "snowball_sql.zip")
            macro_result = run_pre_run_setup(dbname, schemaname, tablename)
            if not macro_result.success:
                print("❌ Pre-run setup macro failed")
                return
                
            run_result = run_dbt(dbname, schemaname, tablename)
            if not run_result.success:
                print("❌ Execution failed")
                return
                
        except Exception as e:
            print(f"❌ Execution failed: {e}")
            return
        
        compile_args = build_dbt_compile_args(dbname, schemaname, tablename)
        compile_result = run_dbt_args(compile_args, dbname, schemaname, tablename)

        if compile_result.success:
            sqlfluff_success = apply_sqlfluff_to_compiled(project_root)
            if not sqlfluff_success:
                with tqdm(desc="SQLFluff issues detected", bar_format='{desc}') as pbar:
                    time.sleep(1)
                    
            if user_choice == 4: process_compiled_sql_files()
            update_revenue_model_with_table_name(compiled_dir, tablename)
            zip_directory(compiled_dir, output_zip)
            print(f"snowball_sql code is generated successfully and saved at: {output_zip}\n")
            print(final_text.center(term_width, '*'))
        else:
            print("❌ dbt compile failed")

    elif user_choice_version == 3:
        print("\nGenerating Spark SQL notebooks...\n")
            
        try:
            output_zip = os.path.join(output_dir, "snowball_spark.zip")
            macro_result = run_pre_run_setup(dbname, schemaname, tablename)
            if not macro_result.success:
                print("❌ Pre-run setup macro failed")
                return
                
            run_result = run_dbt(dbname, schemaname, tablename)
            if not run_result.success:
                print("❌ Execution failed")
                return
                
        except Exception as e:
            print(f"❌ Execution failed: {e}")
            return

        compile_args = build_dbt_compile_args(dbname, schemaname, tablename)
        result = run_dbt_args(compile_args, dbname, schemaname, tablename)

        if result and result.success:
            sqlfluff_success = apply_sqlfluff_to_compiled(project_root)
            if not sqlfluff_success:
                with tqdm(desc="⚠️ SQLFluff issues detected", bar_format='{desc}') as pbar:
                    time.sleep(1)

            generate_notebooks()

            if os.path.exists(output_zip):
                os.remove(output_zip)
            update_revenue_model_with_table_name(notebooks_dir, tablename)
            zip_directory(notebooks_dir, output_zip)
            print(f"Snowball Spark SQL Notebooks are zipped and saved at: {output_zip}\n")
            print(final_text.center(term_width, '*'))

        else:
            print("❌ dbt compile failed")
    elif user_choice_version == 4:
        print("\nRedshift Version is in Progress! Please contact Snowball product team.")
    else:
        print("❌ Invalid choice. Please enter [1-4]")

if __name__ == "__main__":
    main()