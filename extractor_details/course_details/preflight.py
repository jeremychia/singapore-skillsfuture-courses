"""Preflight checks for course details extractor."""

import os
import shutil
import subprocess
import sys

import requests
from course_details.config import BASE_URL, PRIMARY_KEY, PROJECT_ID


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
    """Check if the SkillsFuture API is reachable."""
    try:
        headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        }
        params = {
            "action": "get-course-by-ref-number",
            "refNumber": "TGS-2024000001",  # Test with a sample reference
        }
        response = requests.get(BASE_URL, headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            print(f"✓ API is reachable: {BASE_URL}")
            return True
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


def check_source_table_exists() -> bool:
    """Check if the source courses table exists and has data."""
    try:
        import pandas_gbq

        query = "SELECT COUNT(*) as count FROM `jeremy-chia.sg_skillsfuture.courses`"
        result = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        count = result["count"].iloc[0]
        if count > 0:
            print(f"✓ Source table exists with {count:,} courses")
            return True
        else:
            print("✗ Source table is empty")
            return False
    except Exception as e:
        print(f"✗ Source table check failed: {e}")
        return False


def check_config() -> bool:
    """Validate configuration settings."""
    errors = []

    if not PROJECT_ID:
        errors.append("PROJECT_ID is not set")

    if not BASE_URL:
        errors.append("BASE_URL is not set")

    if not PRIMARY_KEY:
        errors.append("PRIMARY_KEY mapping is empty")

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
        ("Source Table", check_source_table_exists),
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
