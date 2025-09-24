# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Prefect-based data cleaning pipeline for Uganda district data. The system processes diverse CSV datasets including facilities (health, education), demographic data, economic indicators, and other location-aggregated datasets while maintaining consistent location hierarchies and standardized outputs.

## Architecture

### Core Processing Types

The pipeline handles three distinct data processing workflows:

1. **Facility Data Processing** - Individual records per facility with full location hierarchy (District → Subcounty → Parish → Village), coordinate validation, and facility_id generation
2. **Aggregated Data Processing** - Summary data by location (e.g., subcounty population, parish water access) with location-based validation and totals consistency checks
3. **Location Hierarchy Processing** - Extracts and standardizes location names across all data types, generates consistent location codes, and builds unified location reference files

### Project Structure

```
flows/              # Prefect flow definitions for each processing type
tasks/              # Individual Prefect tasks (csv_cleaning, location_processing, facility_processing, etc.)
data/
  ├── raw/          # Input CSVs organized by type (facilities/, demographics/, economic/, infrastructure/)
  └── processed/    # Cleaned outputs with standardized location codes and validation reports
config/             # YAML configuration for data type rules and location mappings
```

### Key Components

- **Data Type Detection**: Auto-classify datasets as facility vs aggregated based on filename/content patterns
- **Thematic Area Mapping**: Automatically identify domain (health, education, water, demographics) from indicators
- **Location Standardization**: Handle location name variations and build consistent hierarchy across all datasets
- **Flexible Location Code Generation**: Hierarchical fallback system (village → parish → subcounty → district) ensures all records get appropriate location codes
- **Validation Rules**: Different validation logic per data type (coordinate validation for facilities, totals consistency for aggregated data)

## Development Commands

### Environment Setup
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Code Quality
```bash
# Format and lint code
python dev_tools.py all      # Run both formatting and linting
python dev_tools.py format   # Run isort and black only
python dev_tools.py lint     # Run flake8 only

# Manual commands
isort tasks/ flows/           # Sort imports
black tasks/ flows/          # Format code
flake8 tasks/ flows/         # Lint code
```

### Running Workflows
```bash
# Test core functionality
python test_simple.py

# Run facility cleaning workflow
python -c "from flows.facility_cleaning_flow import clean_facility_data; clean_facility_data('data/raw/facilities/health/kayunga_health_facilities.csv')"

# Run primary enrollment processing workflow
python -c "from flows.primary_enrollment_processing_flow import process_primary_enrollment_data; process_primary_enrollment_data('data/raw/trends/masindi_primary_school_pupil_enrolment.csv')"

# Batch process multiple files
python -c "from flows.facility_cleaning_flow import batch_clean_facilities; batch_clean_facilities()"
python -c "from flows.primary_enrollment_processing_flow import batch_process_primary_enrollment_data; batch_process_primary_enrollment_data()"
```

## Configuration

The system uses YAML configuration files:
- `data_type_rules.yaml` - Defines required/optional columns and validation rules per data type
- `location_mappings.yaml` - Maps location name variations to standard names

## Output Standards

- All processed data includes standardized location codes
- Facility data gets unique facility_ids and validated coordinates
- Aggregated data maintains sum consistency and percentage bounds
- Master location files (JSON) track all discovered locations across datasets
- Data quality reports provide type-specific validation metrics