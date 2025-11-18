"""
snowball.py

Main operating python file for generating formatted compiled version of snowball project via dbt
and packaging it as per the requirements.
"""
import os
import shutil
from config import *
from tqdm import tqdm
import time
from file_ops import FileOperations
from dbt_operations import DbtOperations
from sql_processor import SQLProcessor
from ui_components import UIComponents


class Snowball:
    def __init__(self):
        self.file_ops = FileOperations()
        self.dbt_ops = DbtOperations()
        self.sql_processor = SQLProcessor()
        self.ui = UIComponents()
        
        self.platform_dict = {
            1: "snowflake",
            2: "databricks",
            3: "fabric",
            4: "sqlserver",
            5: "redshift"
        }

    def setup_initial_environment(self):
        """Set up the initial environment by cloning repo and copying necessary files"""
        # Clone the latest repo from Snowball dbt
        mapping_file_path = self.file_ops.clone_repo("snowball_versions\snowball_dbt")
        self.file_ops.copy_csv_to_downloads(mapping_file_path)

        profiles_src_path = os.path.join(Path(mapping_file_path).parents[1], "profiles.yml")
        self.file_ops.copy_profiles_to_downloads(profiles_src_path)

        self.ui.initial_set_up()

    def get_user_inputs(self):
        """Get all necessary inputs from the user"""
        tablename = input('Enter revenue table name : ')
        db_config = load_dbt_profile("Snowball_dbt", "dev")
        dbname = db_config.get("database")
        schemaname = db_config.get("schema")
        type = db_config.get("type")

        print(f"Revenue table: {dbname}.{schemaname}.{tablename}")

        self.file_ops.update_profile(dbt_profiles_dir, self.file_ops.profiles_dir)
        
        print("Available Database Platform")
        print("     1: Snowflake")
        print("     2: Databricks")
        print("     3: Fabric")
        print("     4: SQL database")
        print("     5: Redshift --In Progress")

        user_choice = int(input("\nSelect your Database Platform [1-5]: ").strip())
        while user_choice not in [1, 2, 3, 4, 5]:
            user_choice = int(input("❌ Invalid input. Please enter [1-5]: ").strip())

        return dbname, schemaname, tablename, user_choice

    def check_connection(self, dbname, schemaname, tablename, user_choice):
        """Check database connection with retry logic"""
        def checking():
            connection = self.dbt_ops.connection_check(dbname, schemaname, tablename)
            db_config = load_dbt_profile("Snowball_dbt", "dev")
            type = db_config.get("type")
            
            if not connection.success or type != self.platform_dict.get(user_choice):
                print("\U0001F641 Connection Failed! \n")
                self.ui.blinking_dots_input("Update Your Profiles.yml correctly and Press Enter to check the connection again!! ")
                checking()
            if connection.success:
                print("\U0001F642 Connection Established Successfully! \n")
        checking()

    def generate_dbt_version(self, dbname, schemaname, tablename, user_choice):
        """Generate dbt version of the project"""
        platform_names = {
            1: "Snowflake", 2: "Databricks", 3: "Fabric", 
            4: "SQL Server", 5: "Redshift"
        }
        text = f"Generating {platform_names.get(user_choice, '')} adaptable dbt code"
        
        self.ui.rotating_slash_after(text, 10)
        output_zip = os.path.join(output_dir, "snowball_dbt.zip")
        self.sql_processor.update_revenue_model_with_table_name(project_dir, tablename)
        self.file_ops.zip_directory(project_dir, output_zip)
        print(f"snowball_dbt code is saved at: {output_zip}\n")

    def generate_sql_version(self, dbname, schemaname, tablename):
        """Generate SQL version of the project"""
        print("Generating SQL code...")
        text = f"Generating sql adaptable Snowball code"
        
        self.ui.rotating_slash_after(text, 10)
        try:
            output_zip = os.path.join(output_dir, "snowball_sql.zip")
            macro_result = self.dbt_ops.run_pre_run_setup(dbname, schemaname, tablename)
            if not macro_result.success:
                print("❌ Pre-run setup macro failed")
                return
                
            run_result = self.dbt_ops.run_dbt(dbname, schemaname, tablename)
            if not run_result.success:
                print("❌ dbt run failed")
                return
                
        except Exception as e:
            print(f"❌ dbt run failed: {e}")
            return
        
        compile_args = self.dbt_ops.build_dbt_compile_args(dbname, schemaname, tablename)
        compile_result = self.dbt_ops.run_dbt_args(compile_args, dbname, schemaname, tablename)

        if compile_result.success:
            sqlfluff_success = self.sql_processor.apply_sqlfluff_to_compiled(project_dir)
            if not sqlfluff_success:
                with tqdm(desc="SQLFluff issues detected", bar_format='{desc}') as pbar:
                    time.sleep(1)
                    
            self.sql_processor.process_compiled_sql_files()
            self.sql_processor.update_revenue_model_with_table_name(self.sql_processor.compiled_dir, tablename)
            self.file_ops.zip_directory(self.sql_processor.compiled_dir, output_zip)
            print(f"snowball_sql code is generated successfully and saved at: {output_zip}\n")
        else:
            print("❌ dbt compile failed")

    def generate_spark_version(self, dbname, schemaname, tablename):
        """Generate Spark SQL version of the project"""
        print("\nGenerating Spark SQL notebooks...\n")
        text = f"Generating spark adaptable Snowball code"
        
        self.ui.rotating_slash_after(text, 10)        
            
        try:
            output_zip = os.path.join(output_dir, "snowball_spark.zip")
            macro_result = self.dbt_ops.run_pre_run_setup(dbname, schemaname, tablename)
            if not macro_result.success:
                print("❌ Pre-run setup macro failed")
                return
                
            run_result = self.dbt_ops.run_dbt(dbname, schemaname, tablename)
            if not run_result.success:
                print("❌ dbt run failed")
                return
                
        except Exception as e:
            print(f"❌ dbt run failed: {e}")
            return

        compile_args = self.dbt_ops.build_dbt_compile_args(dbname, schemaname, tablename)
        result = self.dbt_ops.run_dbt_args(compile_args, dbname, schemaname, tablename)

        if result and result.success:
            sqlfluff_success = self.sql_processor.apply_sqlfluff_to_compiled(project_dir)
            if not sqlfluff_success:
                with tqdm(desc="⚠️ SQLFluff issues detected", bar_format='{desc}') as pbar:
                    time.sleep(1)

            self.sql_processor.generate_notebooks()

            if os.path.exists(output_zip):
                os.remove(output_zip)
            self.sql_processor.update_revenue_model_with_table_name(self.sql_processor.notebooks_dir, tablename)
            self.file_ops.zip_directory(self.sql_processor.notebooks_dir, output_zip)
            print(f"Snowball Spark SQL Notebooks are zipped and saved at: {output_zip}\n")

        else:
            print("❌ dbt compile failed")

    def setup_initial_environment(self):
        """Set up the initial environment by cloning repo and copying necessary files"""
        # Clone the latest repo from Snowball dbt
        mapping_file_path = self.file_ops.clone_repo("snowball_versions\snowball_dbt")
        self.file_ops.copy_csv_to_downloads(mapping_file_path)

        profiles_src_path = os.path.join(Path(mapping_file_path).parents[1], "profiles.yml")
        self.file_ops.copy_profiles_to_downloads(profiles_src_path)

        self.ui.initial_set_up()            

    def main(self):
        """Main execution function"""
        self.ui.welcome_message()

        # Set up initial environment
        self.setup_initial_environment()

        # Connection check section
        self.ui.print_section_header("Checking Database Connection!")
        dbname, schemaname, tablename, user_choice = self.get_user_inputs()
        self.check_connection(dbname, schemaname, tablename, user_choice)

        # Database Platform & Snowball Version section
        self.ui.print_section_header("Database Platform | Snowball Version!")
        
        deps_result = self.dbt_ops.run_dbt_deps(dbname, schemaname, tablename)
        if not deps_result.success:
            print("❌ dbt deps failed")
            return

        self.file_ops.copy_seed_file(mapping_file, self.file_ops.dbt_seed_dir, dbname, schemaname, tablename)
        self.dbt_ops.run_dbt_seed(dbname, schemaname, tablename)
        
        print("Available Snowball Version")
        print("     1: dbt")
        print("     2: sql")
        print("     3: Spark sql")
        print("     4: Redshift - N/A")

        final_text = "  Thanks for using Snowball Product! Happy coding! \U0001F642  "
        term_width = shutil.get_terminal_size().columns
        
        try:
            user_choice_version = int(input("\nSelect your Snowball Version: ").strip())
        except ValueError:
            print("❌ Invalid input. Please enter [1-4].")
            return
        
        # Execute based on user choice
        if user_choice_version == 1:
            self.generate_dbt_version(dbname, schemaname, tablename, user_choice)
        elif user_choice_version == 2:
            self.generate_sql_version(dbname, schemaname, tablename)
        elif user_choice_version == 3:
            self.generate_spark_version(dbname, schemaname, tablename)
        elif user_choice_version == 4:
            print("\nRedshift Version is in Progress! Please contact Snowball product team.")
        else:
            print("❌ Invalid choice. Please enter [1-4]")
            return

        print(final_text.center(term_width, '*'))


if __name__ == "__main__":
    snowball = Snowball()
    snowball.main()