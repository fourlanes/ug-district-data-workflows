import json
from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger

# Import our custom tasks
from tasks.csv_cleaning import (
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
from tasks.location_processing import (
    extract_all_locations,
    generate_unified_location_codes,
    resolve_location_conflicts,
    update_master_locations,
    validate_location_hierarchy,
)
from tasks.validation import (
    detect_duplicate_facilities,
    generate_data_quality_report,
    validate_coordinates,
)

# from prefect.futures import wait  # Not used in this implementation


@flow(name="facility-data-cleaning", log_prints=True)
def clean_facility_data(
    input_file: str,
    output_dir: str = "data/processed/facilities",
    config_dir: str = "config",
) -> dict:
    """
    Main flow for cleaning facility data from CSV files.

    Args:
        input_file: Path to input CSV file
        output_dir: Directory for processed output files
        config_dir: Directory containing configuration files

    Returns:
        Dictionary with processing results and file paths
    """
    logger = get_run_logger()
    logger.info(f"Starting facility data cleaning for: {input_file}")

    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Step 1: Detect data type and thematic area
    logger.info("Step 1: Detecting data type and thematic area...")
    data_type_future = detect_data_type.submit(
        input_file, f"{config_dir}/data_type_rules.yaml"
    )
    thematic_area_future = detect_thematic_area.submit(
        input_file, f"{config_dir}/data_type_rules.yaml"
    )

    # Wait for detection tasks to complete
    data_type = data_type_future.result()
    thematic_area = thematic_area_future.result()

    logger.info(f"Detected data type: {data_type}, thematic area: {thematic_area}")

    # Validate this is facility data
    if data_type != "facility":
        logger.warning(f"Expected facility data but detected: {data_type}")

    # Step 2: Load and clean CSV data
    logger.info("Step 2: Loading and cleaning CSV data...")
    df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(df)} records from {input_file}")

    # Clean headers with context information
    df_clean_headers = standardize_headers(
        df, file_path=input_file, data_type=data_type, thematic_area=thematic_area
    )

    # Clean location data
    df_clean_locations = clean_location_data(
        df_clean_headers, f"{config_dir}/location_mappings.yaml"
    )

    # Clean facility names and officer in charge data
    df_clean_facilities = clean_facility_and_officer_data(df_clean_locations)

    # Generate hierarchical location codes
    df_with_codes = generate_hierarchical_location_codes(
        df_clean_facilities, f"{config_dir}/location_mappings.yaml"
    )

    # Generate facility IDs
    df_with_ids = generate_facility_ids(
        df_with_codes, f"{config_dir}/location_mappings.yaml"
    )

    # Clean NaN values before final output
    df_final = clean_nan_values(df_with_ids)

    logger.info("Data cleaning completed")

    # Step 3: Generate location hierarchy file
    logger.info("Step 3: Generating location hierarchy...")

    # Generate and save hierarchical location file
    hierarchy_future = generate_location_hierarchy_file.submit(
        df_final, "data/processed/locations/location_hierarchy.json", f"{config_dir}/location_mappings.yaml"
    )
    location_hierarchy = hierarchy_future.result()

    logger.info("Location hierarchy generation completed")

    # Step 4: Validation
    logger.info("Step 4: Running validation checks...")

    validation_results = {}

    # Location hierarchy validation
    location_validation_future = validate_location_hierarchy.submit(df_final)
    validation_results["location_validation"] = location_validation_future.result()

    # Coordinate validation
    coordinate_validation_future = validate_coordinates.submit(
        df_final, f"{config_dir}/location_mappings.yaml"
    )
    validation_results["coordinate_validation"] = coordinate_validation_future.result()

    # Duplicate detection
    duplicate_detection_future = detect_duplicate_facilities.submit(
        df_final, f"{config_dir}/location_mappings.yaml"
    )
    validation_results["duplicate_detection"] = duplicate_detection_future.result()

    logger.info("Validation completed")

    # Step 5: Generate quality report
    logger.info("Step 5: Generating data quality report...")

    quality_report_future = generate_data_quality_report.submit(
        data_type, thematic_area, validation_results, input_file
    )
    quality_report = quality_report_future.result()

    # Step 6: Save outputs
    logger.info("Step 6: Saving processed data and reports...")

    # Generate output filenames - use generic name for consolidated file
    thematic_area_name = thematic_area if thematic_area != "general" else "facilities"

    # Save to consolidated CSV file (append if exists, create if not)
    output_csv_path = Path(output_dir) / f"{thematic_area_name}_facilities_cleaned.csv"

    # Check if file exists to determine if we need headers
    file_exists = output_csv_path.exists()

    if file_exists:
        # Append to existing file without headers
        df_final.to_csv(output_csv_path, mode='a', header=False, index=False)
        logger.info(f"Appended {len(df_final)} records to: {output_csv_path}")
    else:
        # Create new file with headers
        df_final.to_csv(output_csv_path, index=False)
        logger.info(f"Created new file with {len(df_final)} records: {output_csv_path}")

    logger.info(f"Location hierarchy saved to: data/processed/locations/location_hierarchy.json")

    # Save quality report with district and timestamp for unique identification
    reports_dir = Path("data/processed/logs")
    reports_dir.mkdir(parents=True, exist_ok=True)

    # Get district name for unique report naming
    district_name = (
        df_final.iloc[0]["district"] if "district" in df_final.columns else "unknown"
    ).lower().replace(" ", "_")

    # Use timestamp to ensure unique report names
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    quality_report_path = reports_dir / f"{district_name}_{thematic_area}_{timestamp}_quality_report.json"

    with open(quality_report_path, "w") as f:
        json.dump(quality_report, f, indent=2)
    logger.info(f"Saved quality report to: {quality_report_path}")

    # Summary results
    results = {
        "status": "completed",
        "input_file": input_file,
        "data_type": data_type,
        "thematic_area": thematic_area,
        "records_processed": len(df_final),
        "output_files": {
            "consolidated_csv": str(output_csv_path),
            "location_hierarchy": "data/processed/locations/location_hierarchy.json",
            "quality_report": str(quality_report_path),
        },
        "file_operation": "appended" if file_exists else "created",
        "quality_score": quality_report["validation_summary"]["overall_quality_score"],
        "validation_summary": validation_results,
    }

    logger.info(
        f"Processing completed successfully. Quality score: {results['quality_score']:.1f}%"
    )
    return results


@flow(name="batch-facility-cleaning")
def batch_clean_facilities(input_directory: str = "data/raw/facilities") -> dict:
    """
    Process multiple facility CSV files in batch.

    Args:
        input_directory: Directory containing CSV files to process

    Returns:
        Dictionary with batch processing results
    """
    logger = get_run_logger()
    input_path = Path(input_directory)

    if not input_path.exists():
        logger.error(f"Input directory does not exist: {input_directory}")
        return {"status": "error", "message": "Input directory not found"}

    # Find all CSV files
    csv_files = list(input_path.rglob("*.csv"))
    logger.info(f"Found {len(csv_files)} CSV files to process")

    if not csv_files:
        logger.warning("No CSV files found in input directory")
        return {"status": "warning", "message": "No CSV files found"}

    # Process each file
    results = {}
    for csv_file in csv_files:
        logger.info(f"Processing: {csv_file}")
        try:
            result = clean_facility_data(str(csv_file))
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

    logger.info(f"Batch processing completed: {successful} successful, {failed} failed")
    return batch_summary


if __name__ == "__main__":
    # Example usage for single file
    result = clean_facility_data(
        "data/raw/facilities/health/kayunga_health_facilities.csv"
    )
    print(f"Processing completed with quality score: {result['quality_score']:.1f}%")
