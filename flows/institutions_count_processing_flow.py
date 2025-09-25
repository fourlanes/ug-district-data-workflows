import json
from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger

# Import our custom tasks
from tasks.csv_cleaning import (
    clean_location_data,
    clean_nan_values,
)
from tasks.location_processing import validate_location_hierarchy


@flow(name="institutions-count-processing", log_prints=True)
def process_institutions_count_data(
    input_file: str,
    output_dir: str = "data/processed/aggregated/institutions_count",
    config_dir: str = "config",
) -> dict:
    """
    Main flow for processing aggregated institutions count data by subcounty.

    This flow is specifically designed for aggregated education data files that contain
    institution counts by subcounty, education level, and ownership type. It adds location
    codes and thematic area designation for consistent data processing.

    Args:
        input_file: Path to input CSV file (institutions count data)
        output_dir: Directory for processed aggregated data output files
        config_dir: Directory containing configuration files

    Returns:
        Dictionary with processing results and file paths
    """
    logger = get_run_logger()
    logger.info(f"Starting institutions count data processing for: {input_file}")

    # Ensure output directories exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Step 1: Load and analyze institutions count data
    logger.info("Step 1: Loading institutions count data...")
    df_original = pd.read_csv(input_file, encoding="utf-8-sig")
    logger.info(
        f"Loaded {len(df_original)} institutions count records from {input_file}"
    )

    # Step 1a: Clean the original institutions count data
    logger.info("Step 1a: Cleaning original institutions count data...")
    df_institutions = df_original.copy()

    # Clean location data using existing task
    df_institutions_clean = clean_location_data(
        df_institutions, f"{config_dir}/location_mappings.yaml"
    )

    # Step 2: Generate location codes for subcounty-level aggregated data
    logger.info("Step 2: Generating location codes for subcounty-level data...")

    # Load existing hierarchy to get location codes at subcounty level
    hierarchy_path = "data/processed/locations/location_hierarchy.json"
    try:
        with open(hierarchy_path, "r") as f:
            hierarchy = json.load(f)

        # Create lookup for subcounty and district levels (no parish/village for aggregated data)
        location_lookup = {}

        for district in hierarchy["districts"]:
            # District level
            district_key = (district["name"], "")
            location_lookup[district_key] = district["code"]

            for subcounty in district["subcounties"]:
                # Subcounty level (primary target for this aggregated data)
                subcounty_key = (district["name"], subcounty["name"])
                location_lookup[subcounty_key] = subcounty["code"]

        # Generate location codes using subcounty as primary, district as fallback
        location_codes = []
        for _, row in df_institutions_clean.iterrows():
            district = str(row.get("district", "")).strip()
            subcounty = str(row.get("subcounty", "")).strip()

            # Clean empty/nan values
            district = district if district and district.lower() != "nan" else ""
            subcounty = subcounty if subcounty and subcounty.lower() != "nan" else ""

            location_code = None

            # Try subcounty level first (most appropriate for aggregated data)
            if district and subcounty:
                key = (district, subcounty)
                location_code = location_lookup.get(key)

            # Try district level as fallback
            if not location_code and district:
                key = (district, "")
                location_code = location_lookup.get(key)

            # Final fallback if no match found
            if not location_code:
                location_code = "UG.UNK.UNK"

            location_codes.append(location_code)

        df_institutions_clean["location_code"] = location_codes

    except Exception as e:
        logger.warning(f"Could not load hierarchy, using fallback location codes: {e}")
        df_institutions_clean["location_code"] = "UG.UNK.UNK"

    # Step 3: Add thematic area designation
    logger.info("Step 3: Adding thematic area designation...")
    df_institutions_clean["thematic_area"] = "education"

    # Step 4: Clean NaN values before final output
    logger.info("Step 4: Cleaning NaN values...")
    df_final = clean_nan_values(df_institutions_clean)

    logger.info("Institutions count data processing completed")

    # Step 5: Validation
    logger.info("Step 5: Running validation checks...")

    validation_results = {}

    # Location hierarchy validation
    location_validation_future = validate_location_hierarchy.submit(df_final)
    validation_results["location_validation"] = location_validation_future.result()

    logger.info("Validation completed")

    # Step 6: Save processed aggregated data
    logger.info("Step 6: Saving processed institutions count data...")

    # Generate output filename based on input file
    input_filename = Path(input_file).stem
    output_path = Path(output_dir) / f"{input_filename}_cleaned.csv"

    # Save cleaned institutions count data
    df_final.to_csv(output_path, index=False, quoting=1)
    logger.info(
        f"Saved cleaned institutions count data with {len(df_final)} records to: {output_path}"
    )

    # Step 7: Generate processing summary report
    logger.info("Step 7: Generating processing summary...")

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
        / f"{district_name}_institutions_count_{timestamp}_processing_report.json"
    )

    # Count unique subcounties and years processed
    unique_subcounties = (
        df_final["subcounty"].nunique() if "subcounty" in df_final.columns else 0
    )
    unique_years = df_final["year"].nunique() if "year" in df_final.columns else 0
    unique_levels = df_final["level"].nunique() if "level" in df_final.columns else 0

    processing_report = {
        "input_file": input_file,
        "processing_type": "aggregated_institutions_count",
        "data_type": "aggregated",
        "thematic_area": "education",
        "records_processed": len(df_original),
        "records_cleaned": len(df_final),
        "unique_subcounties": unique_subcounties,
        "unique_years": unique_years,
        "unique_education_levels": unique_levels,
        "validation_results": validation_results,
        "timestamp": timestamp,
        "output_files": {"cleaned_data_csv": str(output_path)},
    }

    with open(processing_report_path, "w") as f:
        json.dump(processing_report, f, indent=2)
    logger.info(f"Saved processing report to: {processing_report_path}")

    # Summary results
    results = {
        "status": "completed",
        "input_file": input_file,
        "processing_type": "aggregated_institutions_count",
        "data_type": "aggregated",
        "thematic_area": "education",
        "records_processed": len(df_original),
        "records_cleaned": len(df_final),
        "unique_subcounties": unique_subcounties,
        "unique_years": unique_years,
        "unique_education_levels": unique_levels,
        "output_files": {
            "cleaned_data_csv": str(output_path),
            "processing_report": str(processing_report_path),
        },
        "validation_summary": validation_results,
    }

    logger.info(
        f"Institutions count processing completed successfully. "
        f"Processed {len(df_final)} records across {unique_subcounties} subcounties, "
        f"{unique_years} years, and {unique_levels} education levels."
    )
    return results


@flow(name="batch-institutions-count-processing")
def batch_process_institutions_count_data(
    input_directory: str = "data/raw/trends",
) -> dict:
    """
    Process multiple institutions count CSV files in batch.

    Args:
        input_directory: Directory containing institutions count CSV files to process

    Returns:
        Dictionary with batch processing results
    """
    logger = get_run_logger()
    input_path = Path(input_directory)

    if not input_path.exists():
        logger.error(f"Input directory does not exist: {input_directory}")
        return {"status": "error", "message": "Input directory not found"}

    # Find institutions count CSV files (pattern matching)
    csv_files = [
        f
        for f in input_path.rglob("*.csv")
        if "number_of_institutions" in f.name.lower()
        or "institutions_count" in f.name.lower()
    ]
    logger.info(f"Found {len(csv_files)} institutions count CSV files to process")

    if not csv_files:
        logger.warning("No institutions count CSV files found in input directory")
        return {"status": "warning", "message": "No institutions count CSV files found"}

    # Process each file
    results = {}
    for csv_file in csv_files:
        logger.info(f"Processing: {csv_file}")
        try:
            result = process_institutions_count_data(str(csv_file))
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
        f"Batch institutions count processing completed: {successful} successful, {failed} failed"
    )
    return batch_summary


if __name__ == "__main__":
    import sys

    # Example usage for single file - can be made dynamic
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        # Default to Kayunga file for backwards compatibility
        input_file = "data/raw/trends/number_of_institutions_kayunga.csv"

    result = process_institutions_count_data(input_file)
    print(
        f"Processing completed. Processed {result['records_cleaned']} records across {result['unique_subcounties']} subcounties."
    )
