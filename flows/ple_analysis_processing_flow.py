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


@flow(name="ple-analysis-processing", log_prints=True)
def process_ple_analysis_data(
    input_file: str,
    output_dir: str = "data/processed/facilities",
    ple_output_dir: str = "data/processed/ple_analysis",
    config_dir: str = "config",
) -> dict:
    """
    Main flow for processing PLE (Primary Leaving Examination) analysis data and extracting education facilities.

    This flow is specifically designed for PLE analysis data files that contain exam performance
    information and need to be processed to extract unique primary education facilities,
    plus output a cleaned version of the original PLE data with location codes and facility IDs.

    Args:
        input_file: Path to input CSV file (PLE analysis data)
        output_dir: Directory for processed education facilities output files
        ple_output_dir: Directory for processed PLE analysis data output files
        config_dir: Directory containing configuration files

    Returns:
        Dictionary with processing results and file paths
    """
    logger = get_run_logger()
    logger.info(f"Starting PLE analysis data processing for: {input_file}")

    # Ensure output directories exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    Path(ple_output_dir).mkdir(parents=True, exist_ok=True)

    # Step 1: Load and analyze PLE analysis data
    logger.info("Step 1: Loading PLE analysis data...")
    df_original = pd.read_csv(input_file, encoding="utf-8-sig")
    logger.info(f"Loaded {len(df_original)} PLE analysis records from {input_file}")

    # Step 1a: Clean the original PLE analysis data
    logger.info("Step 1a: Cleaning original PLE analysis data...")
    df_ple = df_original.copy()

    # Standardize column names for consistency (handle both uppercase and lowercase variants)
    column_mapping = {
        "Subcounty": "subcounty",
        "Parish": "parish",
        "School_name": "school_name",
        "Ownership": "ownership",
    }
    df_ple = df_ple.rename(columns=column_mapping)

    # Clean location data using existing task
    df_ple_clean = clean_location_data(df_ple, f"{config_dir}/location_mappings.yaml")

    # Generate location codes for PLE analysis data (flexible logic for any location hierarchy level)
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
        for _, row in df_ple_clean.iterrows():
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

        df_ple_clean["location_code"] = location_codes

    except Exception as e:
        logger.warning(f"Could not load hierarchy, using fallback location codes: {e}")
        df_ple_clean["location_code"] = "UG.UNK.UNK"

    df_ple_with_codes = df_ple_clean

    # Generate facility IDs for PLE analysis data
    df_ple_with_ids = generate_facility_ids(
        df_ple_with_codes, f"{config_dir}/location_mappings.yaml"
    )

    # Clean NaN values in PLE analysis data
    df_ple_final = clean_nan_values(df_ple_with_ids)

    logger.info("PLE analysis data cleaning completed")

    # Step 2: Extract unique schools from PLE analysis data
    logger.info("Step 2: Extracting unique primary education facilities...")

    # Determine grouping columns based on available data
    grouping_columns = ["district", "county", "subcounty", "school_name", "ownership"]
    if "parish" in df_original.columns or "Parish" in df_original.columns:
        grouping_columns.insert(3, "parish")  # Insert parish after subcounty

    # Get unique schools (remove duplicates based on available location columns)
    unique_schools = (
        df_original.rename(columns=column_mapping)
        .groupby(grouping_columns)
        .first()
        .reset_index()
    )

    # Remove schools with missing/empty school names
    unique_schools = unique_schools[
        (unique_schools["school_name"].notna())
        & (unique_schools["school_name"].str.strip() != "")
    ]

    logger.info(f"Extracted {len(unique_schools)} unique primary education facilities")

    # Step 3: Transform to education facilities format
    logger.info("Step 3: Transforming to primary education facilities format...")

    # Create education facilities dataframe with required structure
    education_facilities = pd.DataFrame()

    # Map columns according to the specified structure
    education_facilities["district"] = unique_schools["district"]
    education_facilities["county"] = unique_schools["county"]
    education_facilities["subcounty"] = unique_schools["subcounty"]
    education_facilities["parish"] = unique_schools.get(
        "parish", ""
    )  # Available in PLE data
    education_facilities["village"] = ""  # Not typically available in PLE data
    education_facilities["school_name"] = unique_schools["school_name"]
    education_facilities["ownership"] = unique_schools["ownership"]
    education_facilities["institution_type"] = (
        "Primary"  # Fixed value for primary schools
    )
    education_facilities["thematic_area"] = "education"  # Fixed value
    education_facilities["latitude"] = ""  # Not available in PLE data
    education_facilities["longitude"] = ""  # Not available in PLE data

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

    logger.info("Primary education facilities extraction and processing completed")

    # Step 6: Validation
    logger.info("Step 6: Running validation checks...")

    validation_results = {}

    # Location hierarchy validation
    location_validation_future = validate_location_hierarchy.submit(df_final)
    validation_results["location_validation"] = location_validation_future.result()

    logger.info("Validation completed")

    # Step 7: Save education facilities output
    logger.info("Step 7: Saving primary education facilities data...")

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
                    f"Updated {updated_records} and added {new_records} primary education facilities to: {output_csv_path}"
                )
            elif updated_records > 0:
                logger.info(
                    f"Updated {updated_records} existing primary education facilities in: {output_csv_path}"
                )
            else:
                logger.info(
                    f"Added {new_records} new primary education facilities to: {output_csv_path}"
                )
        else:
            # Fallback: if no facility_id, append
            df_final.to_csv(
                output_csv_path, mode="a", header=False, index=False, quoting=1
            )
            logger.info(
                f"Appended {len(df_final)} primary education facilities to: {output_csv_path}"
            )
    else:
        # Create new file with headers
        df_final.to_csv(output_csv_path, index=False, quoting=1)
        logger.info(
            f"Created new primary education facilities file with {len(df_final)} records: {output_csv_path}"
        )

    # Step 7a: Save cleaned PLE analysis data
    logger.info("Step 7a: Saving cleaned PLE analysis data...")

    # Generate PLE analysis output filename based on input file
    input_filename = Path(input_file).stem
    ple_output_path = Path(ple_output_dir) / f"{input_filename}_cleaned.csv"

    # Save cleaned PLE analysis data
    df_ple_final.to_csv(ple_output_path, index=False, quoting=1)
    logger.info(
        f"Saved cleaned PLE analysis data with {len(df_ple_final)} records to: {ple_output_path}"
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
        reports_dir / f"{district_name}_ple_analysis_{timestamp}_processing_report.json"
    )

    processing_report = {
        "input_file": input_file,
        "processing_type": "ple_analysis_to_education_facilities",
        "institution_type": "Primary",
        "ple_records_processed": len(df_original),
        "unique_schools_extracted": len(unique_schools),
        "education_facilities_created": len(df_final),
        "ple_records_cleaned": len(df_ple_final),
        "validation_results": validation_results,
        "timestamp": timestamp,
        "output_files": {
            "education_facilities_csv": str(output_csv_path),
            "cleaned_ple_analysis_csv": str(ple_output_path),
        },
    }

    with open(processing_report_path, "w") as f:
        json.dump(processing_report, f, indent=2)
    logger.info(f"Saved processing report to: {processing_report_path}")

    # Summary results
    results = {
        "status": "completed",
        "input_file": input_file,
        "processing_type": "ple_analysis_to_education_facilities",
        "institution_type": "Primary",
        "ple_records_processed": len(df_original),
        "unique_schools_extracted": len(unique_schools),
        "education_facilities_created": len(df_final),
        "ple_records_cleaned": len(df_ple_final),
        "output_files": {
            "education_facilities_csv": str(output_csv_path),
            "cleaned_ple_analysis_csv": str(ple_output_path),
            "processing_report": str(processing_report_path),
        },
        "file_operation": "updated" if file_exists else "created",
        "validation_summary": validation_results,
    }

    logger.info(
        f"PLE analysis processing completed successfully. "
        f"Extracted {len(df_final)} primary education facilities from {len(df_original)} PLE records. "
        f"Saved cleaned PLE analysis data with {len(df_ple_final)} records."
    )
    return results


@flow(name="combine-ple-analysis")
def combine_ple_analysis_data(
    output_path: str = "data/processed/ple_analysis/ple_analysis_combined.csv",
) -> dict:
    """
    Combine all processed PLE analysis CSV files into a single combined file.

    Args:
        output_path: Path for the combined output CSV file

    Returns:
        Dictionary with combination results
    """
    logger = get_run_logger()
    logger.info("Starting PLE analysis data combination...")

    # Find all processed PLE analysis files
    ple_analysis_dir = Path("data/processed/ple_analysis")
    if not ple_analysis_dir.exists():
        logger.error("PLE analysis directory does not exist")
        return {"status": "error", "message": "PLE analysis directory not found"}

    # Find all cleaned PLE analysis files
    csv_files = list(ple_analysis_dir.glob("*_ple_analysis_cleaned.csv"))
    logger.info(f"Found {len(csv_files)} processed PLE analysis files to combine")

    if not csv_files:
        logger.warning("No processed PLE analysis files found")
        return {"status": "warning", "message": "No processed PLE analysis files found"}

    # Combine all files
    combined_data = []
    source_info = []

    for csv_file in csv_files:
        logger.info(f"Processing: {csv_file}")
        try:
            df = pd.read_csv(csv_file)
            # Add source file information
            df["source_file"] = csv_file.name
            combined_data.append(df)
            source_info.append({"file": csv_file.name, "records": len(df)})
        except Exception as e:
            logger.error(f"Error reading {csv_file}: {str(e)}")

    if not combined_data:
        logger.error("No data could be loaded from any files")
        return {"status": "error", "message": "No data could be loaded"}

    # Combine all dataframes
    combined_df = pd.concat(combined_data, ignore_index=True)

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Save combined data
    combined_df.to_csv(output_path, index=False, quoting=1)

    total_records = len(combined_df)
    logger.info(
        f"Combined {len(csv_files)} files with {total_records} total records to: {output_path}"
    )

    results = {
        "status": "completed",
        "combined_file": output_path,
        "total_files": len(csv_files),
        "total_records": total_records,
        "source_files": source_info,
    }

    return results


@flow(name="batch-ple-analysis-processing")
def batch_process_ple_analysis_data(input_directory: str = "data/raw/trends") -> dict:
    """
    Process multiple PLE analysis CSV files in batch.

    Args:
        input_directory: Directory containing PLE analysis CSV files to process

    Returns:
        Dictionary with batch processing results
    """
    logger = get_run_logger()
    input_path = Path(input_directory)

    if not input_path.exists():
        logger.error(f"Input directory does not exist: {input_directory}")
        return {"status": "error", "message": "Input directory not found"}

    # Find PLE analysis CSV files (pattern matching)
    csv_files = [
        f
        for f in input_path.rglob("*.csv")
        if "ple" in f.name.lower() and "analysis" in f.name.lower()
    ]
    logger.info(f"Found {len(csv_files)} PLE analysis CSV files to process")

    if not csv_files:
        logger.warning("No PLE analysis CSV files found in input directory")
        return {"status": "warning", "message": "No PLE analysis CSV files found"}

    # Process each file
    results = {}
    for csv_file in csv_files:
        logger.info(f"Processing: {csv_file}")
        try:
            result = process_ple_analysis_data(str(csv_file))
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
        f"Batch PLE analysis processing completed: {successful} successful, {failed} failed"
    )

    # If processing was successful, create combined file
    if successful > 0:
        logger.info("Creating combined PLE analysis file...")
        combine_result = combine_ple_analysis_data()
        batch_summary["combine_result"] = combine_result

    return batch_summary


if __name__ == "__main__":
    import sys

    # Example usage for single file - can be made dynamic
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        # Default to Kayunga file for backwards compatibility
        input_file = "data/raw/trends/kayunga_ple_analysis.csv"

    result = process_ple_analysis_data(input_file)
    print(
        f"Processing completed. Extracted {result['education_facilities_created']} primary education facilities."
    )
    print(f"Cleaned PLE analysis data: {result['ple_records_cleaned']} records.")
