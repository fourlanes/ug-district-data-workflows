import json
from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger

# Import our custom tasks
from tasks.csv_cleaning import (
    clean_location_data,
    detect_data_type,
    detect_thematic_area,
    generate_facility_ids,
    generate_location_codes,
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

    # Clean headers
    df_clean_headers = standardize_headers(df)

    # Clean location data
    df_clean_locations = clean_location_data(
        df_clean_headers, f"{config_dir}/location_mappings.yaml"
    )

    # Generate location codes
    df_with_codes = generate_location_codes(
        df_clean_locations, f"{config_dir}/data_type_rules.yaml"
    )

    # Generate facility IDs
    df_final = generate_facility_ids(
        df_with_codes, f"{config_dir}/location_mappings.yaml"
    )

    logger.info("Data cleaning completed")

    # Step 3: Location processing
    logger.info("Step 3: Processing location hierarchy...")

    # Extract locations
    locations_future = extract_all_locations.submit(
        df_final, f"{config_dir}/location_mappings.yaml"
    )
    locations = locations_future.result()

    # Resolve conflicts and generate codes
    resolved_locations_future = resolve_location_conflicts.submit(locations)
    resolved_locations = resolved_locations_future.result()

    coded_locations_future = generate_unified_location_codes.submit(resolved_locations)
    coded_locations = coded_locations_future.result()

    # Update master locations
    master_locations_future = update_master_locations.submit(coded_locations)
    master_locations_future.result()  # Ensure completion

    logger.info("Location processing completed")

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

    # Generate output filenames
    input_filename = Path(input_file).stem
    district_name = (
        df_final.iloc[0]["District"] if "District" in df_final.columns else "unknown"
    )

    # Save cleaned CSV
    output_csv_path = Path(output_dir) / f"{input_filename}_cleaned.csv"
    df_final.to_csv(output_csv_path, index=False)
    logger.info(f"Saved cleaned data to: {output_csv_path}")

    # Save location hierarchy for this district
    district_locations_dir = Path("data/processed/locations/district_locations")
    district_locations_dir.mkdir(parents=True, exist_ok=True)
    district_locations_path = (
        district_locations_dir / f"{district_name.lower()}_locations.json"
    )

    with open(district_locations_path, "w") as f:
        json.dump(coded_locations, f, indent=2)
    logger.info(f"Saved district locations to: {district_locations_path}")

    # Save quality report
    reports_dir = Path("data/processed/logs")
    reports_dir.mkdir(parents=True, exist_ok=True)
    quality_report_path = reports_dir / f"{input_filename}_quality_report.json"

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
            "cleaned_csv": str(output_csv_path),
            "district_locations": str(district_locations_path),
            "quality_report": str(quality_report_path),
        },
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
