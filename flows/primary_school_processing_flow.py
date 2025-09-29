import json
from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger, task

# Import our custom tasks
from tasks.csv_cleaning import (
    add_thematic_area_column,
    clean_facility_and_officer_data,
    clean_location_data,
    clean_nan_values,
    detect_data_type,
    detect_thematic_area,
    generate_facility_ids,
    generate_hierarchical_location_codes,
    generate_location_hierarchy_file,
    standardize_headers,
)
from tasks.location_processing import validate_location_hierarchy
from tasks.validation import (
    detect_duplicate_facilities,
    generate_data_quality_report,
    validate_coordinates,
)


@task(retries=3)
def load_and_preprocess_primary_school_data(
    file_path: str, column_mappings: dict, excluded_columns: list
) -> pd.DataFrame:
    """
    Load primary school data and apply initial preprocessing.

    Args:
        file_path: Path to the CSV file
        column_mappings: Dictionary mapping source to destination column names
        excluded_columns: List of column patterns to exclude

    Returns:
        Preprocessed DataFrame
    """
    logger = get_run_logger()
    logger.info(f"Loading primary school data from: {file_path}")

    # Load data
    df = pd.read_csv(file_path, encoding="utf-8-sig")
    logger.info(f"Loaded {len(df)} records with {len(df.columns)} columns")

    # Remove excluded columns
    columns_to_drop = []
    for col in df.columns:
        for pattern in excluded_columns:
            # Use exact match for specific exclusions to avoid accidentally excluding latitude/longitude
            if pattern.lower() == col.lower() or (
                pattern.lower() in col.lower()
                and "latitude" not in col.lower()
                and "longitude" not in col.lower()
            ):
                columns_to_drop.append(col)
                break

    if columns_to_drop:
        logger.info(f"Excluding {len(columns_to_drop)} columns: {columns_to_drop}")
        df = df.drop(columns=columns_to_drop)

    # Apply column mappings
    rename_dict = {}
    for source_col, dest_col in column_mappings.items():
        if source_col in df.columns:
            rename_dict[source_col] = dest_col
        else:
            logger.warning(f"Column '{source_col}' not found in data")

    if rename_dict:
        logger.info(f"Renaming {len(rename_dict)} columns")
        df = df.rename(columns=rename_dict)

    return df


@task(retries=3)
def enhance_location_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enhanced location cleaning for primary school data with additional patterns.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with enhanced location cleaning
    """
    logger = get_run_logger()
    df_cleaned = df.copy()

    location_columns = ["district", "subcounty", "parish", "village"]

    for col in location_columns:
        if col in df_cleaned.columns:
            logger.info(f"Cleaning location column: {col}")

            # Apply standard camel case formatting first
            df_cleaned[col] = df_cleaned[col].astype(str).str.strip()
            df_cleaned[col] = df_cleaned[col].apply(
                lambda x: _format_camel_case(x) if pd.notna(x) and x != "nan" else ""
            )

            # Define qualifiers to remove based on column type
            if col == "subcounty":
                qualifiers_to_remove = [
                    " Subcounty",
                    " subcounty",
                    " Sub County",
                    " sub county",
                    " TC",
                    " tc",
                    "TC ",
                    "tc ",
                    "Tc",
                    " SC",
                    " sc",
                    "SC ",
                    "sc ",
                    "Sc",
                    "Subcounty ",
                    "subcounty ",
                    "Sub County ",
                    "sub county ",
                    "TC",
                    "tc",
                    "SC",
                    "sc",
                ]
            elif col == "parish":
                qualifiers_to_remove = [
                    " Parish",
                    " parish",
                    "Parish ",
                    "parish ",
                    "Parish",
                    "parish",
                ]
            elif col == "village":
                qualifiers_to_remove = [
                    " Village",
                    " village",
                    "Village ",
                    "village ",
                    "Village",
                    "village",
                ]
            else:
                qualifiers_to_remove = []

            # Remove qualifiers
            for qualifier in qualifiers_to_remove:
                df_cleaned[col] = df_cleaned[col].str.replace(
                    qualifier, "", regex=False
                )

            # Clean up any extra spaces
            df_cleaned[col] = df_cleaned[col].str.replace(r"\s+", " ", regex=True)
            df_cleaned[col] = df_cleaned[col].str.strip()

    return df_cleaned


@task(retries=3)
def clean_school_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean school names with standard formatting.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with cleaned school names
    """
    logger = get_run_logger()
    df_cleaned = df.copy()

    if "school_name" in df_cleaned.columns:
        logger.info("Cleaning school names")

        # Apply standard camel case formatting
        df_cleaned["school_name"] = df_cleaned["school_name"].astype(str).str.strip()
        df_cleaned["school_name"] = df_cleaned["school_name"].apply(
            lambda x: _format_camel_case(x) if pd.notna(x) and x != "nan" else ""
        )

        # Fix common abbreviations and patterns - use regex for better boundary matching

        # Define replacement patterns with word boundaries
        replacements = [
            (r"\bP/S\b", "Primary School"),
            (r"\bPS\b", "Primary School"),
            (r"\bSCH\b", "School"),
            (r"\bCU\b", "Church of Uganda"),
            (r"\bC/U\b", "Church of Uganda"),
            (r"\bRC\b", "Roman Catholic"),
            (r"\bR/C\b", "Roman Catholic"),
            (r"\bNPS\b", "Nursery and Primary School"),
            (r"\bN/PS\b", "Nursery and Primary School"),
            (r"\bNURSERY\b", "Nursery"),
        ]

        for pattern, replacement in replacements:
            df_cleaned["school_name"] = df_cleaned["school_name"].str.replace(
                pattern, replacement, regex=True, case=False
            )

        # Remove extra spaces
        df_cleaned["school_name"] = df_cleaned["school_name"].str.replace(
            r"\s+", " ", regex=True
        )
        df_cleaned["school_name"] = df_cleaned["school_name"].str.strip()

    return df_cleaned


@task(retries=3)
def merge_with_existing_facilities(
    new_df: pd.DataFrame,
    existing_file: str = "data/processed/facilities/education_facilities.csv",
) -> pd.DataFrame:
    """
    Merge new primary school data with existing facilities, updating existing records.

    Args:
        new_df: New primary school data
        existing_file: Path to existing facilities file

    Returns:
        Merged DataFrame with updated and new records
    """
    logger = get_run_logger()

    # Load existing data if it exists
    existing_df = pd.DataFrame()
    if Path(existing_file).exists():
        existing_df = pd.read_csv(existing_file, encoding="utf-8-sig")
        logger.info(f"Loaded {len(existing_df)} existing facilities")
    else:
        logger.info("No existing facilities file found, creating new one")

    if len(existing_df) == 0:
        # No existing data, return new data with institution_type
        result_df = new_df.copy()
        result_df["institution_type"] = "Primary"
        return result_df

    # Identify matches based on facility_id (which now uses subcounty format)
    merged_records = []
    new_records = []

    for _, new_row in new_df.iterrows():
        # Look for existing facility with same facility_id
        if "facility_id" in new_row.index and pd.notna(new_row["facility_id"]):
            mask = existing_df["facility_id"] == str(new_row["facility_id"])
            existing_matches = existing_df[mask]

            if len(existing_matches) > 0:
                # Update existing record
                existing_idx = existing_matches.index[0]
                updated_row = existing_df.loc[existing_idx].copy()

                # Update with new data (preserve existing facility_id but allow location_code updates)
                for col in new_row.index:
                    if col != "facility_id" and pd.notna(new_row[col]):
                        updated_row[col] = new_row[col]

                # Update location details to the more accurate ones from new data
                location_cols = ["district", "subcounty", "parish", "village"]
                for col in location_cols:
                    if (
                        col in new_row.index
                        and pd.notna(new_row[col])
                        and str(new_row[col]).strip()
                    ):
                        updated_row[col] = new_row[col]

                updated_row["institution_type"] = "Primary"
                merged_records.append(updated_row)

                # Remove from existing_df to avoid duplicates
                existing_df = existing_df.drop(existing_idx)

                logger.info(f"Updated existing school: {new_row['school_name']}")
            else:
                # New record
                new_record = new_row.copy()
                new_record["institution_type"] = "Primary"
                new_records.append(new_record)
                logger.info(f"Added new school: {new_row['school_name']}")
        else:
            # No facility_id in new row, treat as new record
            new_record = new_row.copy()
            new_record["institution_type"] = "Primary"
            new_records.append(new_record)
            logger.info(f"Added new school (no facility_id): {new_row.get('school_name', 'Unknown')}")

    # Combine all records
    result_parts = []

    if len(existing_df) > 0:
        result_parts.append(existing_df)

    if merged_records:
        merged_df = pd.DataFrame(merged_records)
        result_parts.append(merged_df)

    if new_records:
        new_df_final = pd.DataFrame(new_records)
        result_parts.append(new_df_final)

    if result_parts:
        result_df = pd.concat(result_parts, ignore_index=True)
    else:
        result_df = pd.DataFrame()

    # Apply school name cleaning to entire merged dataset to ensure consistency
    if len(result_df) > 0 and "school_name" in result_df.columns:
        logger.info("Applying school name cleaning to merged dataset")
        result_df = clean_school_names_in_dataframe(result_df)

    # Apply location cleaning to entire merged dataset to ensure consistency
    if len(result_df) > 0:
        logger.info("Applying location cleaning to merged dataset")
        result_df = enhance_location_cleaning(result_df)

    logger.info(f"Final dataset: {len(result_df)} total facilities")
    logger.info(f"- Updated: {len(merged_records)} existing facilities")
    logger.info(f"- Added: {len(new_records)} new facilities")

    return result_df


def clean_school_names_in_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply school name cleaning to a dataframe.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with cleaned school names
    """
    df_cleaned = df.copy()

    if "school_name" in df_cleaned.columns:
        # Apply standard camel case formatting
        df_cleaned["school_name"] = df_cleaned["school_name"].astype(str).str.strip()
        df_cleaned["school_name"] = df_cleaned["school_name"].apply(
            lambda x: _format_camel_case(x) if pd.notna(x) and x != "nan" else ""
        )

        # Fix common abbreviations and patterns - use regex for better boundary matching

        # Define replacement patterns with word boundaries
        replacements = [
            (r"\bP/S\b", "Primary School"),
            (r"\bPS\b", "Primary School"),
            (r"\bSCH\b", "School"),
            (r"\bCU\b", "Church of Uganda"),
            (r"\bC/U\b", "Church of Uganda"),
            (r"\bRC\b", "Roman Catholic"),
            (r"\bR/C\b", "Roman Catholic"),
            (r"\bNPS\b", "Nursery and Primary School"),
            (r"\bN/PS\b", "Nursery and Primary School"),
            (r"\bNURSERY\b", "Nursery"),
        ]

        for pattern, replacement in replacements:
            df_cleaned["school_name"] = df_cleaned["school_name"].str.replace(
                pattern, replacement, regex=True, case=False
            )

        # Remove extra spaces
        df_cleaned["school_name"] = df_cleaned["school_name"].str.replace(
            r"\s+", " ", regex=True
        )
        df_cleaned["school_name"] = df_cleaned["school_name"].str.strip()

    return df_cleaned


def _format_camel_case(text: str) -> str:
    """
    Convert text to Camel Case format (first letter of each word capitalized).
    """
    if pd.isna(text) or not isinstance(text, str):
        return str(text)

    text = str(text).strip()

    # Handle special cases like abbreviations (keep them uppercase)
    words = text.split()
    formatted_words = []

    for word in words:
        # Keep abbreviations uppercase if they are 2-3 characters
        if len(word) <= 3 and word.isupper():
            formatted_words.append(word)
        else:
            formatted_words.append(word.capitalize())

    return " ".join(formatted_words)


@flow(name="primary-school-data-processing", log_prints=True)
def process_primary_school_data(
    input_file: str,
    output_file: str = "data/processed/facilities/education_facilities.csv",
    config_dir: str = "config",
) -> dict:
    """
    Main flow for processing primary school data and merging with existing facilities.

    Args:
        input_file: Path to input CSV file
        output_file: Path to output facilities file
        config_dir: Directory containing configuration files

    Returns:
        Dictionary with processing results and file paths
    """
    logger = get_run_logger()
    logger.info(f"Starting primary school data processing for: {input_file}")

    # Define column mappings - you can customize these destination names
    column_mappings = {
        "District name": "district",
        "Sub county": "subcounty",
        "Parish": "parish",
        "Village": "village",
        "Name of the school": "school_name",
        "School type": "ownership",
        "_Geo location of the school_latitude": "latitude",
        "_Geo location of the school_longitude": "longitude",
        "Total number of learners": "total_learners",
        "Number of boys": "boys_count",
        "Number of girls": "girls_count",
        "Number of learners with disabilities (boys/girls)": "disabled_learners",
        "Total number of teachers": "total_teachers",
        "Number of male teachers": "male_teachers",
        "Number of Female teachers": "female_teachers",
        "Number of teachers on government payroll ": "govt_payroll_teachers",  # Note the trailing space
        "Number of teachers qualified by Diploma": "diploma_teachers",
        "Number of teachers qualified degree and above": "degree_teachers",
        "Number of teachers qualified by certificate": "certificate_teachers",
        "Number of classroom blocks": "classroom_blocks",
        "Of these, how many are permanent classrooms": "permanent_classrooms",
        "How many are semi-permanent classrooms": "semi_permanent_classrooms",
        "How many are temporary classrooms": "temporary_classrooms",
        "Number of Toilets": "total_toilets",
        "Are there separate toilets for boys and girls? ": "separate_gender_toilets",  # Note the trailing space
        "if yes, Number of toilets available for boys": "boys_toilets",
        "if yes, Number of toilets available for girls": "girls_toilets",
        "Are there separate toilets for teachers": "teacher_toilets_available",
        "if yes, Number of toilets available for teachers": "teacher_toilets_count",
        "Are toilets accommodative for people with Disabilities e.g walking": "disability_accessible_toilets",
        "Is water available at the school? ": "water_available",  # Note the trailing space
        "Is there a handwashing point available at school": "handwashing_available",
        "Does the school have electricity? ": "electricity_available",  # Note the trailing space
        "Does the school have an ICT/computer lab? ": "ict_lab_available",  # Note the trailing space
        "On average, how many pupils does one classroom accommodate /how many learners sit in one classroom (Pupil-classroom ratio)": "pupil_classroom_ratio",
        "On average, how pupils does on teacher attend to /teach in this school (Average pupil–teacher ratio)": "pupil_teacher_ratio",
        "Pupil-text book ratio-on average one book is used by how many pupils?": "pupil_textbook_ratio",
        "Pupil–toilet ratio( On average, how many pupils use one toilet ?": "pupil_toilet_ratio",
    }

    # Define columns to exclude - you can modify this list
    excluded_columns = [
        "photo",
        "image",
        "_url",
        "take a photo",
        "_id",
        "_uuid",
        "_submission_time",
        "_validation_status",
        "_notes",
        "_status",
        "_submitted_by",
        "__version__",
        "_tags",
        "_index",
        "does the school have pre-primary education",
        "pre-primary education",
        "Geo location of the school",
        "_Geo location of the school_altitude",
        "_Geo location of the school_precision",
    ]

    # Ensure output directory exists
    output_dir = Path(output_file).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Load and preprocess data
    df_raw = load_and_preprocess_primary_school_data(
        input_file, column_mappings, excluded_columns
    )

    # Step 2: Detect data type and thematic area
    data_type = detect_data_type(input_file, f"{config_dir}/data_type_rules.yaml")
    thematic_area = detect_thematic_area(
        input_file, f"{config_dir}/data_type_rules.yaml"
    )
    logger.info(f"Detected data type: {data_type}, thematic area: {thematic_area}")

    # Step 3: Enhanced location cleaning
    df_locations = enhance_location_cleaning(df_raw)

    # Step 4: Clean school names
    df_schools = clean_school_names(df_locations)

    # Step 5: Clean other data issues
    df_clean = clean_nan_values(df_schools)

    # Step 6: Generate location codes
    df_coded = generate_hierarchical_location_codes(
        df_clean, f"{config_dir}/location_mappings.yaml"
    )

    # Step 7: Generate facility IDs
    df_with_ids = generate_facility_ids(
        df_coded, f"{config_dir}/location_mappings.yaml"
    )

    # Step 8: Add thematic area
    df_final = add_thematic_area_column(df_with_ids, thematic_area)

    # Step 9: Merge with existing facilities
    df_merged = merge_with_existing_facilities(df_final, output_file)

    # Step 10: Validate coordinates
    validation_results = validate_coordinates(df_merged)

    # Step 11: Generate data quality report
    quality_report = generate_data_quality_report(
        data_type,
        thematic_area,
        validation_results,
        str(output_file).replace(".csv", "_quality_report.json"),
    )

    # Step 12: Update location hierarchy with new locations (after all cleaning is complete)
    hierarchy = generate_location_hierarchy_file(
        df_merged,
        "data/processed/locations/location_hierarchy.json",
        f"{config_dir}/location_mappings.yaml",
    )

    # Save final output
    df_merged.to_csv(output_file, index=False, encoding="utf-8-sig")
    logger.info(f"Saved processed data to: {output_file}")

    return {
        "status": "success",
        "input_file": input_file,
        "output_file": str(output_file),
        "records_processed": len(df_merged),
        "data_type": data_type,
        "thematic_area": thematic_area,
        "quality_report": quality_report,
        "validation_results": validation_results,
        "location_hierarchy_updated": len(hierarchy.get("districts", [])),
    }


@flow(name="batch-primary-school-processing", log_prints=True)
def batch_process_primary_school_data(
    input_pattern: str = "data/raw/facilities/education/*primary_school_dataset.csv",
    output_file: str = "data/processed/facilities/education_facilities.csv",
    config_dir: str = "config",
) -> dict:
    """
    Batch process multiple primary school datasets.

    Args:
        input_pattern: Glob pattern for input files
        output_file: Path to output facilities file
        config_dir: Directory containing configuration files

    Returns:
        Dictionary with batch processing results
    """
    logger = get_run_logger()

    # Find all matching files
    input_files = list(Path().glob(input_pattern))
    logger.info(
        f"Found {len(input_files)} files to process: {[str(f) for f in input_files]}"
    )

    if not input_files:
        logger.warning(f"No files found matching pattern: {input_pattern}")
        return {"status": "no_files_found", "pattern": input_pattern}

    results = []
    total_records = 0

    # Process each file
    for file_path in input_files:
        logger.info(f"Processing file: {file_path}")

        try:
            result = process_primary_school_data(
                str(file_path), output_file, config_dir
            )
            results.append(result)
            total_records += result.get("records_processed", 0)

        except Exception as e:
            logger.error(f"Failed to process {file_path}: {str(e)}")
            results.append(
                {"status": "failed", "input_file": str(file_path), "error": str(e)}
            )

    successful_files = [r for r in results if r.get("status") == "success"]
    failed_files = [r for r in results if r.get("status") == "failed"]

    return {
        "status": "completed",
        "total_files": len(input_files),
        "successful_files": len(successful_files),
        "failed_files": len(failed_files),
        "total_records_processed": total_records,
        "output_file": output_file,
        "results": results,
    }
