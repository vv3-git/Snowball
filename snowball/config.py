"""
config.py

    This configuration file contains all your static values required for the Snowball ARR project.
    You can define constants such as project paths, environment settings, update mapping csv, platform / adapter
    and other parameters that are reused across the project.
"""
import yaml
from pathlib import Path

# === Project paths === #
dbt_profiles_dir = str(Path.home() / "Downloads/profiles.yml")
project_dir  = str(Path.home() / "Downloads/snowball_dbt")
mapping_file = str(Path.home() / "Downloads/column_mapping.csv")
output_dir   = str(Path.home() / "Downloads")


def load_dbt_profile(profile_name: str = "Snowball_dbt", target: str = "dev") -> dict:
    """
    Load DBT profile credentials from the profiles.yml file in Downloads.

    Supports Databricks, SQL Server, Fabric, Snowflake, and Redshift adapters.
    Returns a simplified dictionary with key connection info.
    """
    profiles_path = Path(dbt_profiles_dir)

    if not profiles_path.exists():
        raise FileNotFoundError(f"profiles.yml not found at: {profiles_path}")

    try:
        with open(profiles_path, "r") as file:
            profiles = yaml.safe_load(file) or {}

        profile = profiles.get(profile_name)
        if not profile:
            raise ValueError(f"Profile '{profile_name}' not found in profiles.yml")

        outputs = profile.get("outputs", {})
        target_profile = outputs.get(target)
        if not target_profile:
            raise ValueError(f"Target '{target}' not found under profile '{profile_name}'")

        platform_type = target_profile.get("type", "").lower()

        if platform_type == "sqlserver":
            db_name = target_profile.get("database", "")
        elif platform_type == "databricks":
            db_name = target_profile.get("catalog", "")
        else:
            db_name = target_profile.get("database", target_profile.get("dbname", ""))

        db_vars = {
            "platform": platform_type,
            "database": db_name,
            "schema": target_profile.get("schema", ""),
            "type": platform_type,
            "threads": target_profile.get("threads", 1)
        }

        # print(f"Loaded DB profile successfully as: {db_vars}") 
        return db_vars

    except Exception as e:
        raise Exception(f"Error loading DBT profile from {profiles_path}: {e}")
