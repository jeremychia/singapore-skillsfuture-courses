# singapore-skillsfuture-courses

Dataset for SkillsFuture Courses in Singapore

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/jeremychia/singapore-skillsfuture-courses.git
   cd singapore-skillsfuture-courses
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Add your GCP credentials to `tokens/gcp_token.json`

## Usage

### Running the Pipeline

Use `run.sh` to execute the data pipeline:

```bash
./run.sh [OPTIONS]
```

**Options:**

| Flag | Description |
|------|-------------|
| `-e`, `--extractors` | Run extraction scripts (courses & course_details) |
| `-m`, `--modelling` | Run modelling/transformation scripts |
| `-a`, `--all` | Run full pipeline (extractors + modelling) |
| `-h`, `--help` | Show help message |

**Examples:**

```bash
# Run only extractors (fetch data from SkillsFuture API)
./run.sh -e

# Run only modelling (transform data in BigQuery)
./run.sh -m

# Run full pipeline
./run.sh -a
```

### Pipeline Stages

#### Stage 1: Extractors
Fetches data from the SkillsFuture API and loads it into BigQuery:
- `extractor_courses/` - Extracts course listings
- `extractor_details/` - Extracts detailed course information

#### Stage 2: Modelling
Transforms raw data into analytical models in BigQuery:
- `courses.py` - Course-level aggregations
- `course_runs.py` - Course run details
- `training_providers.py` - Provider-level metrics
- `training_locations.py` - Location-based analysis
- `trainers.py` - Trainer information

## Google Colab (Legacy)

Refer to notebooks for source to scrape tables and analysis:

* Courses: [Google Colab](https://colab.research.google.com/drive/1oxb37kA_xILJsESV5UT-TdGqvnd5XcV-?authuser=1#scrollTo=0kcG25Dgsxnk)
* Get Longitude and Latitude from OneMap API: [Google Colab](https://colab.research.google.com/drive/1QtN9TMy7DaN06lmZ_GlvON2KPMsIwE0K?authuser=1#scrollTo=ApXTHLdPRSBK)
* Distribution of Skillsfuture Courses on the Map: [Google Colab](https://colab.research.google.com/drive/1dRy6p9jYaifV_BlW1w3SdiCBTfUN29tl?authuser=1)