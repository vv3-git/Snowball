from setuptools import setup, find_packages
import os

this_directory = os.path.abspath(os.path.dirname(__file__))
try:
    with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = ""

setup(
    name="snowball",
    version="1.4.6",
    description="Generate dbt, sql projects and PySpark notebooks.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Vishal Verma",
    author_email="vishal.verma@jmangroup.com",
    url="https://gitserver/org/dbt_runner",
    packages=find_packages(),
    include_package_data=True,   # <-- let MANIFEST.in do all the work
    install_requires=[
        "GitPython>=3.1.0",
        "dbt-core>=1.5.0",
        "sqlfluff>=0.13.0",
        "nbformat>=5.0.0",
        "tqdm>=4.60.0",
        "dbt-sqlserver==1.9.0",
        "dbt-fabric",
        "dbt-snowflake",
        "dbt-databricks"
    ],
    entry_points={
        'console_scripts': [
            'snowball = snowball.snowball:main',
        ],
    },
    python_requires='>=3.7',
)
