"""Preflight checks for courses extractor."""

import os
import shutil
import subprocess
import sys

import requests
from courses.config import API_HEADERS, API_URL, BIGQUERY_TABLES, PRIMARY_KEYS, PROJECT_ID


def check_uv_installed() -> bool:
    """Check if uv is installed and available."""
    if shutil.which("uv"):
        try:
            result = subprocess.run(
                ["uv", "--version"], capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                version = result.stdout.strip()
                print(f"✓ uv is installed: {version}")
                return True
        except subprocess.TimeoutExpired:
            print("✗ uv command timed out")
            return False
    print("✗ uv is not installed. Install with: curl -LsSf https://astral.sh/uv/install.sh | sh")
    return False


def check_uv_environment() -> bool:
    """Check if uv virtual environment is set up correctly."""
    try:
        # Check if we're in a uv-managed environment
        result = subprocess.run(
            ["uv", "pip", "list"], capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            print("✓ uv environment is active")
            return True
        else:
            print(f"✗ uv environment check failed: {result.stderr}")
            return False
    except FileNotFoundError:
        print("✗ uv not found")
        return False
    except subprocess.TimeoutExpired:
        print("✗ uv pip list timed out")
        return False


def check_dependencies_installed() -> bool:
    """Check if required dependencies are installed via uv."""
    required_packages = ["pandas", "pandas-gbq", "requests", "google-cloud-bigquery"]
    try:
        result = subprocess.run(
            ["uv", "pip", "list"], capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            print("✗ Could not list installed packages")
            return False

        installed = result.stdout.lower()
        missing = [pkg for pkg in required_packages if pkg.replace("-", "_").lower() not in installed.replace("-", "_")]

        if missing:
            print(f"✗ Missing packages: {', '.join(missing)}")
            print(f"  Install with: uv pip install {' '.join(missing)}")
            return False
        else:
            print(f"✓ All required packages installed")
            return True
    except Exception as e:
        print(f"✗ Dependency check failed: {e}")
        return False


def check_credentials_file(credentials_path: str = "tokens/gcp_token.json") -> bool:
    """Check if GCP credentials file exists."""
    if os.path.exists(credentials_path):
        print(f"✓ GCP credentials file found: {credentials_path}")
        return True
    else:
        print(f"✗ GCP credentials file not found: {credentials_path}")
        return False


def check_api_connectivity() -> bool:
    """Check if the SkillsFuture course search API is reachable."""
    try:
        params = {"query": "rows=1&facet=true&facet.mincount=1&json.nl=map&start=0"}
        response = requests.get(API_URL, headers=API_HEADERS, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if "grouped" in data:
                print(f"✓ API is reachable: {API_URL}")
                return True
            else:
                print("✗ API response does not contain expected data structure")
                return False
        else:
            print(f"✗ API returned status code: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ API connection failed: {e}")
        return False


def check_bigquery_connectivity() -> bool:
    """Check if BigQuery is accessible."""
    try:
        import pandas_gbq

        # Try a simple query to verify connectivity
        test_query = "SELECT 1 as test"
        result = pandas_gbq.read_gbq(test_query, project_id=PROJECT_ID)
        if len(result) > 0:
            print(f"✓ BigQuery connection successful (project: {PROJECT_ID})")
            return True
        else:
            print("✗ BigQuery query returned no results")
            return False
    except Exception as e:
        print(f"✗ BigQuery connection failed: {e}")
        return False


def check_config() -> bool:
    """Validate configuration settings."""
    errors = []

    if not PROJECT_ID:
        errors.append("PROJECT_ID is not set")

    if not API_URL:
        errors.append("API_URL is not set")

    if not BIGQUERY_TABLES:
        errors.append("BIGQUERY_TABLES mapping is empty")

    if not PRIMARY_KEYS:
        errors.append("PRIMARY_KEYS mapping is empty")

    # Check that all tables have corresponding primary keys
    for table_name in BIGQUERY_TABLES:
        if table_name not in PRIMARY_KEYS:
            errors.append(f"Missing PRIMARY_KEY for table: {table_name}")

    if errors:
        for error in errors:
            print(f"✗ Config error: {error}")
        return False
    else:
        print("✓ Configuration is valid")
        return True


def run_preflight(
    credentials_path: str = "tokens/gcp_token.json", exit_on_failure: bool = True
) -> bool:
    """
    Run all preflight checks.

    Args:
        credentials_path: Path to GCP credentials file
        exit_on_failure: If True, exit the program on preflight failure

    Returns:
        True if all checks pass, False otherwise
    """
    print("=" * 50)
    print("Running preflight checks...")
    print("=" * 50)

    # Set credentials environment variable for checks
    if os.path.exists(credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    checks = [
        ("uv Installation", check_uv_installed),
        ("uv Environment", check_uv_environment),
        ("Dependencies", check_dependencies_installed),
        ("Credentials File", lambda: check_credentials_file(credentials_path)),
        ("Configuration", check_config),
        ("API Connectivity", check_api_connectivity),
        ("BigQuery Connectivity", check_bigquery_connectivity),
    ]

    results = []
    for check_name, check_func in checks:
        print(f"\n[{check_name}]")
        results.append(check_func())

    print("\n" + "=" * 50)

    if all(results):
        print("✓ All preflight checks passed!")
        print("=" * 50 + "\n")
        return True
    else:
        failed_checks = [
            checks[i][0] for i, passed in enumerate(results) if not passed
        ]
        print(f"✗ Preflight checks failed: {', '.join(failed_checks)}")
        print("=" * 50 + "\n")
        if exit_on_failure:
            sys.exit(1)
        return False
