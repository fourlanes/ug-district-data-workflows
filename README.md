# Uganda District Data Workflows

A Prefect-based data cleaning pipeline for Uganda district data. The system processes diverse CSV datasets including facilities (health, education), demographic data, economic indicators, and other location-aggregated datasets while maintaining consistent location hierarchies and standardized outputs.

## Project Overview

This pipeline handles three distinct data processing workflows:

1. **Facility Data Processing** - Individual records per facility with full location hierarchy, coordinate validation, and unique facility ID generation
2. **Aggregated Data Processing** - Summary data by location with validation and consistency checks
3. **Location Hierarchy Processing** - Extracts and standardizes location names, generates consistent location codes with unique hierarchical IDs

### Key Features

- **Flexible Location Code Generation**: Hierarchical fallback system (village → parish → subcounty → district) ensures all records get appropriate location codes
- **Cross-District Compatibility**: Dynamic processing flows handle multiple districts (Kayunga, Masindi) without modification
- **Column Name Standardization**: Automatically handles both uppercase and lowercase column name variants
- **Unique Hierarchical Location IDs**: URL-friendly IDs that prevent collisions (e.g., `d-kayunga`, `s-kayunga-kangulumira-town-council`)
- **Smart Duplicate Prevention**: Updates existing facility records instead of creating duplicates
- **Data Type Auto-Detection**: Automatically classifies datasets as facility vs aggregated data
- **Thematic Area Mapping**: Identifies domain (health, education, water, etc.) from indicators
- **Comprehensive Validation**: Location hierarchy validation, coordinate checks, and data quality reporting

## Setup

### Environment Setup
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Running Workflows

### Facility Data Cleaning

#### Single File Processing
```bash
python -c "from flows.facility_cleaning_flow import clean_facility_data; clean_facility_data('data/raw/facilities/health/kayunga_health_facilities.csv')"
```

#### Batch Processing Multiple Files
```bash
python -c "from flows.facility_cleaning_flow import batch_clean_facilities; batch_clean_facilities()"
```

### Primary Enrollment Data Processing

#### Single File Processing
```bash
python -c "from flows.primary_enrollment_processing_flow import process_primary_enrollment_data; process_primary_enrollment_data('data/raw/trends/masindi_primary_school_pupil_enrolment.csv')"
```

#### Batch Processing Multiple Files
```bash
python -c "from flows.primary_enrollment_processing_flow import batch_process_primary_enrollment_data; batch_process_primary_enrollment_data()"
```

### PLE Analysis Data Processing

#### Single File Processing
```bash
# Process Kayunga PLE analysis
python -c "from flows.ple_analysis_processing_flow import process_ple_analysis_data; process_ple_analysis_data('data/raw/trends/kayunga_ple_analysis.csv')"

# Process Masindi PLE analysis
python -c "from flows.ple_analysis_processing_flow import process_ple_analysis_data; process_ple_analysis_data('data/raw/trends/masindi_ple_analysis.csv')"
```

#### Batch Processing Multiple Files
```bash
python -c "from flows.ple_analysis_processing_flow import batch_process_ple_analysis_data; batch_process_ple_analysis_data()"
```

#### Testing Core Functionality
```bash
python test_simple.py
```

### Output Files

- **Education Facilities**: `data/processed/facilities/education_facilities.csv`
- **Cleaned Enrollment Data**: `data/processed/enrollment/{input_filename}_cleaned.csv`
- **Cleaned PLE Analysis Data**: `data/processed/ple_analysis/{input_filename}_cleaned.csv`
- **Location Hierarchy**: `data/processed/locations/location_hierarchy.json`
- **Processing Reports**: `data/processed/logs/{district}_{processing_type}_{timestamp}_processing_report.json`

## Code Quality

### Automated Code Quality Checks
```bash
# Run all checks (format + lint)
python dev_tools.py all

# Format code only (isort + black)
python dev_tools.py format

# Lint code only (flake8)
python dev_tools.py lint
```

### Manual Commands
```bash
# Sort imports
isort tasks/ flows/

# Format code
black tasks/ flows/

# Lint code
flake8 tasks/ flows/
```

## Configuration

The system uses YAML configuration files:
- `config/data_type_rules.yaml` - Data type detection and validation rules
- `config/location_mappings.yaml` - Location name standardization mappings
- `config/column_mappings.yaml` - Column header standardization

## Project Structure

```
flows/              # Prefect flow definitions
tasks/              # Individual Prefect tasks (csv_cleaning, validation, etc.)
data/
  ├── raw/          # Input CSVs (facilities/, demographics/, economic/, infrastructure/)
  └── processed/    # Cleaned outputs with standardized location codes
config/             # YAML configuration files
```

## Output Standards

- All processed data includes standardized location codes and unique hierarchical location IDs
- Facility data gets unique facility_ids and validated coordinates
- Aggregated data maintains sum consistency and percentage bounds
- Master location files (JSON) track all discovered locations across datasets
- Data quality reports provide type-specific validation metrics

## Development

See [CLAUDE.md](CLAUDE.md) for detailed development guidelines and architecture information.
