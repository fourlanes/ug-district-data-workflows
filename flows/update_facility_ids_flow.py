import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect.logging import get_run_logger
from slugify import slugify


@task(retries=3)
def update_facility_ids_to_subcounty_format(
    input_file: str = "data/processed/facilities/education_facilities.csv",
    backup_file: str = "data/processed/facilities/education_facilities_backup.csv"
) -> pd.DataFrame:
    """
    Update facility IDs in education_facilities.csv to use subcounty format.

    Changes format from: {facility-name-slug}_{district}
    To: {facility-name-slug}_{subcounty}

    Args:
        input_file: Path to the education facilities file
        backup_file: Path to save backup

    Returns:
        Updated DataFrame with new facility IDs
    """
    logger = get_run_logger()

    # Load existing data
    if not Path(input_file).exists():
        logger.warning(f"File {input_file} does not exist")
        return pd.DataFrame()

    df = pd.read_csv(input_file, encoding="utf-8-sig")
    logger.info(f"Loaded {len(df)} facilities from {input_file}")

    # Create backup
    Path(backup_file).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(backup_file, index=False, encoding="utf-8-sig")
    logger.info(f"Created backup at {backup_file}")

    # Check required columns
    if "school_name" not in df.columns:
        logger.error("school_name column not found")
        return df

    if "subcounty" not in df.columns:
        logger.error("subcounty column not found")
        return df

    # Generate new facility IDs using subcounty
    updated_ids = []
    id_changes = 0

    for _, row in df.iterrows():
        facility_name = str(row["school_name"]) if pd.notna(row["school_name"]) else "unknown"
        facility_slug = slugify(facility_name, separator="-")

        subcounty_name = str(row["subcounty"]) if pd.notna(row["subcounty"]) else "unknown"
        subcounty_slug = slugify(subcounty_name, separator="-")

        new_facility_id = f"{facility_slug}_{subcounty_slug}"
        updated_ids.append(new_facility_id)

        # Check if ID changed
        if "facility_id" in df.columns and str(row["facility_id"]) != new_facility_id:
            id_changes += 1

    # Update facility_id column
    df["facility_id"] = updated_ids

    logger.info(f"Updated {id_changes} facility IDs to use subcounty format")
    logger.info(f"Total facilities processed: {len(df)}")

    return df


@task(retries=3)
def remove_duplicate_facilities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate facilities based on the new facility_id.
    Keeps the first occurrence of each unique facility_id.

    Args:
        df: DataFrame with facility data

    Returns:
        DataFrame with duplicates removed
    """
    logger = get_run_logger()

    if len(df) == 0:
        return df

    initial_count = len(df)

    # Find duplicates based on facility_id
    duplicates = df[df.duplicated(subset=["facility_id"], keep=False)]

    if len(duplicates) > 0:
        logger.info(f"Found {len(duplicates)} duplicate records:")

        # Group duplicates by facility_id
        for facility_id, group in duplicates.groupby("facility_id"):
            logger.info(f"  {facility_id}: {len(group)} duplicates")
            # Show location details for each duplicate
            for _, row in group.iterrows():
                logger.info(f"    - {row.get('subcounty', 'N/A')} / {row.get('parish', 'N/A')}")

    # Remove duplicates, keeping first occurrence
    df_deduplicated = df.drop_duplicates(subset=["facility_id"], keep="first")

    removed_count = initial_count - len(df_deduplicated)
    logger.info(f"Removed {removed_count} duplicate facilities")
    logger.info(f"Final count: {len(df_deduplicated)} unique facilities")

    return df_deduplicated


@task(retries=3)
def save_updated_facilities(
    df: pd.DataFrame,
    output_file: str = "data/processed/facilities/education_facilities.csv"
) -> str:
    """
    Save the updated facilities data to file.

    Args:
        df: Updated DataFrame
        output_file: Path to save the file

    Returns:
        Path to saved file
    """
    logger = get_run_logger()

    # Ensure output directory exists
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    # Save to CSV
    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    logger.info(f"Saved {len(df)} facilities to {output_file}")
    return output_file


@flow(name="update-facility-ids")
def update_facility_ids_flow(
    input_file: str = "data/processed/facilities/education_facilities.csv",
    backup_file: str = "data/processed/facilities/education_facilities_backup.csv"
):
    """
    Flow to update facility IDs in education_facilities.csv to use subcounty format
    and remove any duplicates created by the ID changes.

    Args:
        input_file: Path to the education facilities file to update
        backup_file: Path to save backup before updating
    """
    print("🔄 Starting facility ID update process...")
    print(f"   Input file: {input_file}")
    print(f"   Backup file: {backup_file}")

    # Update facility IDs to subcounty format
    updated_df = update_facility_ids_to_subcounty_format(
        input_file=input_file,
        backup_file=backup_file
    )

    if len(updated_df) == 0:
        print("❌ No data to process")
        return

    # Remove duplicates based on new facility IDs
    print("🧹 Removing duplicates based on new facility IDs...")
    deduplicated_df = remove_duplicate_facilities(updated_df)

    # Save updated file
    print("💾 Saving updated facilities...")
    output_file = save_updated_facilities(deduplicated_df, input_file)

    print("✅ Facility ID update completed!")
    print(f"   Updated file: {output_file}")
    print(f"   Backup available at: {backup_file}")

    return {
        "output_file": output_file,
        "backup_file": backup_file,
        "total_facilities": len(deduplicated_df),
        "original_count": len(updated_df)
    }


if __name__ == "__main__":
    # For testing
    result = update_facility_ids_flow()
    print(f"\n📊 Final Report:")
    print(f"   Original facilities: {result.get('original_count', 0)}")
    print(f"   Final facilities: {result.get('total_facilities', 0)}")
    print(f"   Duplicates removed: {result.get('original_count', 0) - result.get('total_facilities', 0)}")