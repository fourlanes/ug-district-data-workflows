import json
from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger

# Import our custom tasks
from tasks.csv_cleaning import (
    clean_location_data,
    clean_nan_values,
    generate_facility_ids,
    generate_hierarchical_location_codes,
)
from tasks.location_processing import validate_location_hierarchy


@flow(name="secondary-enrollment-processing", log_prints=True)
def process_secondary_enrollment_data(
    input_file: str,
    output_dir: str = "data/processed/facilities",
    enrollment_output_dir: str = "data/processed/enrollment",
    config_dir: str = "config",
) -> dict:
    """
    Main flow for processing secondary enrollment data and extracting education facilities.

    This flow is specifically designed for enrollment data files that contain school information
    and need to be processed to extract unique education facilities, plus output a cleaned
    version of the original enrollment data with location codes and facility IDs.

    Args:
        input_file: Path to input CSV file (enrollment data)
        output_dir: Directory for processed education facilities output files
        enrollment_output_dir: Directory for processed enrollment data output files
        config_dir: Directory containing configuration files

    Returns:
        Dictionary with processing results and file paths
    """
    logger = get_run_logger()
    logger.info(f"Starting secondary enrollment data processing for: {input_file}")

    # Ensure output directories exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    Path(enrollment_output_dir).mkdir(parents=True, exist_ok=True)

    # Step 1: Load and analyze enrollment data
    logger.info("Step 1: Loading enrollment data...")
    df_original = pd.read_csv(input_file, encoding="utf-8-sig")
    logger.info(f"Loaded {len(df_original)} enrollment records from {input_file}")

    # Step 1a: Clean the original enrollment data
    logger.info("Step 1a: Cleaning original enrollment data...")
    df_enrollment = df_original.copy()

    # Rename subtotal column to total
    if "subtotal" in df_enrollment.columns:
        df_enrollment = df_enrollment.rename(columns={"subtotal": "total"})

    # Clean location data using existing task
    df_enrollment_clean = clean_location_data(
        df_enrollment, f"{config_dir}/location_mappings.yaml"
    )

    # Generate location codes for enrollment data (flexible logic for any location hierarchy level)
    logger.info(
        "Generating location codes from existing hierarchy using most specific location available..."
    )

    # Load existing hierarchy to get location codes at all levels
    hierarchy_path = "data/processed/locations/location_hierarchy.json"
    try:
        with open(hierarchy_path, "r") as f:
            hierarchy = json.load(f)

        # Create comprehensive lookup for all location levels
        location_lookup = {}

        for district in hierarchy["districts"]:
            # District level
            district_key = (district["name"], "", "", "")
            location_lookup[district_key] = district["code"]

            for subcounty in district["subcounties"]:
                # Subcounty level
                subcounty_key = (district["name"], subcounty["name"], "", "")
                location_lookup[subcounty_key] = subcounty["code"]

                for parish in subcounty.get("parishes", []):
                    # Parish level
                    parish_key = (
                        district["name"],
                        subcounty["name"],
                        parish["name"],
                        "",
                    )
                    location_lookup[parish_key] = parish["code"]

                    for village in parish.get("villages", []):
                        # Village level
                        village_key = (
                            district["name"],
                            subcounty["name"],
                            parish["name"],
                            village["name"],
                        )
                        location_lookup[village_key] = village["code"]

        # Generate location codes using most specific available location
        location_codes = []
        for _, row in df_enrollment_clean.iterrows():
            district = str(row.get("district", "")).strip()
            subcounty = str(row.get("subcounty", "")).strip()
            parish = str(row.get("parish", "")).strip()
            village = str(row.get("village", "")).strip()

            # Clean empty/nan values
            district = district if district and district.lower() != "nan" else ""
            subcounty = subcounty if subcounty and subcounty.lower() != "nan" else ""
            parish = parish if parish and parish.lower() != "nan" else ""
            village = village if village and village.lower() != "nan" else ""

            # Try most specific to least specific location combinations
            location_code = None

            # Try village level first (most specific)
            if district and subcounty and parish and village:
                key = (district, subcounty, parish, village)
                location_code = location_lookup.get(key)

            # Try parish level
            if not location_code and district and subcounty and parish:
                key = (district, subcounty, parish, "")
                location_code = location_lookup.get(key)

            # Try subcounty level
            if not location_code and district and subcounty:
                key = (district, subcounty, "", "")
                location_code = location_lookup.get(key)

            # Try district level
            if not location_code and district:
                key = (district, "", "", "")
                location_code = location_lookup.get(key)

            # Fallback if no match found
            if not location_code:
                location_code = "UG.UNK.UNK"

            location_codes.append(location_code)

        df_enrollment_clean["location_code"] = location_codes

    except Exception as e:
        logger.warning(f"Could not load hierarchy, using fallback location codes: {e}")
        df_enrollment_clean["location_code"] = "UG.UNK.UNK"

    df_enrollment_with_codes = df_enrollment_clean

    # Generate facility IDs for enrollment data
    df_enrollment_with_ids = generate_facility_ids(
        df_enrollment_with_codes, f"{config_dir}/location_mappings.yaml"
    )

    # Clean NaN values in enrollment data
    df_enrollment_final = clean_nan_values(df_enrollment_with_ids)

    logger.info("Enrollment data cleaning completed")

    # Step 2: Extract unique schools from enrollment data
    logger.info("Step 2: Extracting unique education facilities...")

    # Get unique schools (remove duplicates based on school_name and subcounty)
    unique_schools = (
        df_original.groupby(
            ["district", "county", "subcounty", "school_name", "ownership"]
        )
        .first()
        .reset_index()
    )

    # Remove schools with missing/empty school names
    unique_schools = unique_schools[
        (unique_schools["school_name"].notna())
        & (unique_schools["school_name"].str.strip() != "")
    ]

    logger.info(f"Extracted {len(unique_schools)} unique education facilities")

    # Step 3: Transform to education facilities format
    logger.info("Step 3: Transforming to education facilities format...")

    # Create education facilities dataframe with required structure
    education_facilities = pd.DataFrame()

    # Map columns according to the specified structure
    education_facilities["district"] = unique_schools["district"]
    education_facilities["county"] = unique_schools["county"]
    education_facilities["subcounty"] = unique_schools["subcounty"]
    education_facilities["parish"] = ""  # Not available in enrollment data
    education_facilities["village"] = ""  # Not available in enrollment data
    education_facilities["school_name"] = unique_schools["school_name"]
    education_facilities["ownership"] = unique_schools["ownership"]
    education_facilities["institution_type"] = (
        "Secondary"  # Fixed value for secondary enrollment data
    )
    education_facilities["thematic_area"] = "education"  # Fixed value
    education_facilities["latitude"] = ""  # Not available in enrollment data
    education_facilities["longitude"] = ""  # Not available in enrollment data

    # Step 4: Clean and standardize location data
    logger.info("Step 4: Cleaning and standardizing location data...")

    # Clean location data using existing task
    df_clean_locations = clean_location_data(
        education_facilities, f"{config_dir}/location_mappings.yaml"
    )

    # Step 5: Generate location codes and facility IDs
    logger.info("Step 5: Generating location codes and facility IDs...")

    # Generate hierarchical location codes using existing hierarchy
    df_with_codes = generate_hierarchical_location_codes(
        df_clean_locations, f"{config_dir}/location_mappings.yaml"
    )

    # Generate facility IDs using existing task
    df_with_ids = generate_facility_ids(
        df_with_codes, f"{config_dir}/location_mappings.yaml"
    )

    # Clean NaN values before final output
    df_final = clean_nan_values(df_with_ids)

    logger.info("Education facilities extraction and processing completed")

    # Step 6: Validation
    logger.info("Step 6: Running validation checks...")

    validation_results = {}

    # Location hierarchy validation
    location_validation_future = validate_location_hierarchy.submit(df_final)
    validation_results["location_validation"] = location_validation_future.result()

    logger.info("Validation completed")

    # Step 7: Save education facilities output
    logger.info("Step 7: Saving education facilities data...")

    # Save to education facilities CSV file
    output_csv_path = Path(output_dir) / "education_facilities.csv"

    # Check if file exists to determine if we need to merge or create
    file_exists = output_csv_path.exists()

    if file_exists:
        # Load existing data and merge with new data
        existing_df = pd.read_csv(output_csv_path)

        # Merge/update based on facility_id (unique identifier)
        if "facility_id" in df_final.columns and "facility_id" in existing_df.columns:
            # Remove existing records with same facility_ids to avoid duplicates
            existing_df = existing_df[
                ~existing_df["facility_id"].isin(df_final["facility_id"])
            ]

            # Concatenate existing (without duplicates) + new data
            merged_df = pd.concat([existing_df, df_final], ignore_index=True)

            # Count new vs updated records
            original_existing_ids = (
                pd.read_csv(output_csv_path)["facility_id"]
                if output_csv_path.exists()
                else pd.Series([], dtype=str)
            )
            new_records = len(
                df_final[~df_final["facility_id"].isin(original_existing_ids)]
            )
            updated_records = len(df_final) - new_records

            merged_df.to_csv(output_csv_path, index=False, quoting=1)

            if updated_records > 0 and new_records > 0:
                logger.info(
                    f"Updated {updated_records} and added {new_records} education facilities to: {output_csv_path}"
                )
            elif updated_records > 0:
                logger.info(
                    f"Updated {updated_records} existing education facilities in: {output_csv_path}"
                )
            else:
                logger.info(
                    f"Added {new_records} new education facilities to: {output_csv_path}"
                )
        else:
            # Fallback: if no facility_id, append
            df_final.to_csv(
                output_csv_path, mode="a", header=False, index=False, quoting=1
            )
            logger.info(
                f"Appended {len(df_final)} education facilities to: {output_csv_path}"
            )
    else:
        # Create new file with headers
        df_final.to_csv(output_csv_path, index=False, quoting=1)
        logger.info(
            f"Created new education facilities file with {len(df_final)} records: {output_csv_path}"
        )

    # Step 7a: Save cleaned enrollment data
    logger.info("Step 7a: Saving cleaned enrollment data...")

    # Generate enrollment output filename based on input file
    input_filename = Path(input_file).stem
    enrollment_output_path = (
        Path(enrollment_output_dir) / f"{input_filename}_cleaned.csv"
    )

    # Save cleaned enrollment data
    df_enrollment_final.to_csv(enrollment_output_path, index=False, quoting=1)
    logger.info(
        f"Saved cleaned enrollment data with {len(df_enrollment_final)} records to: {enrollment_output_path}"
    )

    # Step 8: Generate processing summary report
    logger.info("Step 8: Generating processing summary...")

    # Save processing report
    reports_dir = Path("data/processed/logs")
    reports_dir.mkdir(parents=True, exist_ok=True)

    # Get district name for unique report naming
    district_name = (
        (df_final.iloc[0]["district"] if "district" in df_final.columns else "unknown")
        .lower()
        .replace(" ", "_")
    )

    # Use timestamp to ensure unique report names
    from datetime import datetime

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    processing_report_path = (
        reports_dir
        / f"{district_name}_secondary_enrollment_{timestamp}_processing_report.json"
    )

    processing_report = {
        "input_file": input_file,
        "processing_type": "secondary_enrollment_to_education_facilities",
        "enrollment_records_processed": len(df_original),
        "unique_schools_extracted": len(unique_schools),
        "education_facilities_created": len(df_final),
        "enrollment_records_cleaned": len(df_enrollment_final),
        "validation_results": validation_results,
        "timestamp": timestamp,
        "output_files": {
            "education_facilities_csv": str(output_csv_path),
            "cleaned_enrollment_csv": str(enrollment_output_path),
        },
    }

    with open(processing_report_path, "w") as f:
        json.dump(processing_report, f, indent=2)
    logger.info(f"Saved processing report to: {processing_report_path}")

    # Summary results
    results = {
        "status": "completed",
        "input_file": input_file,
        "processing_type": "secondary_enrollment_to_education_facilities",
        "enrollment_records_processed": len(df_original),
        "unique_schools_extracted": len(unique_schools),
        "education_facilities_created": len(df_final),
        "enrollment_records_cleaned": len(df_enrollment_final),
        "output_files": {
            "education_facilities_csv": str(output_csv_path),
            "cleaned_enrollment_csv": str(enrollment_output_path),
            "processing_report": str(processing_report_path),
        },
        "file_operation": "updated" if file_exists else "created",
        "validation_summary": validation_results,
    }

    logger.info(
        f"Secondary enrollment processing completed successfully. "
        f"Extracted {len(df_final)} education facilities from {len(df_original)} enrollment records. "
        f"Saved cleaned enrollment data with {len(df_enrollment_final)} records."
    )
    return results


@flow(name="batch-secondary-enrollment-processing")
def batch_process_secondary_enrollment_data(
    input_directory: str = "data/raw/trends",
) -> dict:
    """
    Process multiple secondary enrollment CSV files in batch.

    Args:
        input_directory: Directory containing secondary enrollment CSV files to process

    Returns:
        Dictionary with batch processing results
    """
    logger = get_run_logger()
    input_path = Path(input_directory)

    if not input_path.exists():
        logger.error(f"Input directory does not exist: {input_directory}")
        return {"status": "error", "message": "Input directory not found"}

    # Find secondary enrollment CSV files (pattern matching)
    csv_files = [
        f
        for f in input_path.rglob("*.csv")
        if "secondary" in f.name.lower()
        and ("enrollment" in f.name.lower() or "enrolment" in f.name.lower())
    ]
    logger.info(f"Found {len(csv_files)} secondary enrollment CSV files to process")

    if not csv_files:
        logger.warning("No secondary enrollment CSV files found in input directory")
        return {
            "status": "warning",
            "message": "No secondary enrollment CSV files found",
        }

    # Process each file
    results = {}
    for csv_file in csv_files:
        logger.info(f"Processing: {csv_file}")
        try:
            result = process_secondary_enrollment_data(str(csv_file))
            results[str(csv_file)] = result
        except Exception as e:
            logger.error(f"Error processing {csv_file}: {str(e)}")
            results[str(csv_file)] = {"status": "error", "error_message": str(e)}

    # Generate batch summary
    successful = sum(1 for r in results.values() if r.get("status") == "completed")
    failed = len(results) - successful

    batch_summary = {
        "status": "completed",
        "total_files": len(csv_files),
        "successful": successful,
        "failed": failed,
        "individual_results": results,
    }

    logger.info(
        f"Batch secondary enrollment processing completed: {successful} successful, {failed} failed"
    )
    return batch_summary


if __name__ == "__main__":
    # Example usage for single file
    result = process_secondary_enrollment_data(
        "data/raw/trends/kayunga_learners_enrolment_secondary.csv"
    )
    print(
        f"Processing completed. Extracted {result['education_facilities_created']} education facilities."
    )
    print(f"Cleaned enrollment data: {result['enrollment_records_cleaned']} records.")
