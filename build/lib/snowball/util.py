"""
utils.py

Common utility functions used across multiple modules.
"""
import os
from config import *


def get_paths():
    """Get common paths used across the application"""
    return {
        'compiled_dir': os.path.join(project_dir, "target", "compiled"),
        'dbt_seed_dir': os.path.join(project_dir, "seeds"),
        'profiles_dir': str(Path.home() / "Downloads/snowball_dbt"),
        'notebooks_dir': os.path.join(output_dir, "notebooks")
    }