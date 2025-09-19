# Uganda District Data Workflows

A Prefect-based data cleaning pipeline for Uganda district data. The system processes diverse CSV datasets including facilities (health, education), demographic data, economic indicators, and other location-aggregated datasets while maintaining consistent location hierarchies and standardized outputs.

## Project Overview

This pipeline handles three distinct data processing workflows:

1. **Facility Data Processing** - Individual records per facility with full location hierarchy, coordinate validation, and unique facility ID generation
2. **Aggregated Data Processing** - Summary data by location with validation and consistency checks
3. **Location Hierarchy Processing** - Extracts and standardizes location names, generates consistent location codes with unique hierarchical IDs

### Key Features

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

#### Testing Core Functionality
```bash
python test_simple.py
```

### Output Files

- **Cleaned CSV**: `data/processed/facilities/{thematic_area}_facilities_cleaned.csv`
- **Location Hierarchy**: `data/processed/locations/location_hierarchy.json`
- **Quality Reports**: `data/processed/logs/{district}_{thematic_area}_{timestamp}_quality_report.json`

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
