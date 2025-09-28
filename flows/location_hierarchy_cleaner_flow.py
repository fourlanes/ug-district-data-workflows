from prefect import flow

from tasks.location_hierarchy_cleaner import (
    clean_location_hierarchy_duplicates,
    validate_cleaned_hierarchy,
)


@flow(name="location-hierarchy-cleaner")
def clean_hierarchy_duplicates(
    hierarchy_path: str = "data/processed/locations/location_hierarchy.json",
    backup_path: str = "data/processed/locations/location_hierarchy_backup.json",
):
    """
    Flow to clean duplicate locations from the hierarchy file.

    This flow:
    1. Creates a backup of the existing hierarchy
    2. Removes duplicate locations with the same name but different IDs
    3. Regenerates all IDs using consistent format (d-*, s-*, p-*, v-*)
    4. Preserves all existing hierarchical codes (D01S01P01V01 format)
    5. Validates the cleaned hierarchy

    Args:
        hierarchy_path: Path to the hierarchy file to clean
        backup_path: Path to save backup before cleaning
    """
    print("🧹 Starting location hierarchy cleaning process...")
    print(f"   Input file: {hierarchy_path}")
    print(f"   Backup will be saved to: {backup_path}")

    # Clean the hierarchy
    cleaned_hierarchy = clean_location_hierarchy_duplicates(
        hierarchy_path=hierarchy_path, backup_path=backup_path
    )

    print("✅ Hierarchy cleaning completed")

    # Validate the results
    print("🔍 Validating cleaned hierarchy...")
    validation_report = validate_cleaned_hierarchy(hierarchy_path=hierarchy_path)

    if validation_report["validation_passed"]:
        print("🎉 Success! Hierarchy is now clean with no duplicates.")
        print(
            f"   Final counts: {validation_report['total_districts']} districts, "
            f"{validation_report['total_subcounties']} subcounties, "
            f"{validation_report['total_parishes']} parishes, "
            f"{validation_report['total_villages']} villages"
        )
    else:
        print("⚠️  Warning: Some duplicates may still remain.")
        if validation_report.get("duplicate_subcounties"):
            print(
                f"   Duplicate subcounties: {len(validation_report['duplicate_subcounties'])}"
            )
        if validation_report.get("duplicate_parishes"):
            print(
                f"   Duplicate parishes: {len(validation_report['duplicate_parishes'])}"
            )
        if validation_report.get("duplicate_villages"):
            print(
                f"   Duplicate villages: {len(validation_report['duplicate_villages'])}"
            )

    return {
        "cleaned_hierarchy": cleaned_hierarchy,
        "validation_report": validation_report,
        "backup_created": backup_path,
    }


if __name__ == "__main__":
    # For testing
    result = clean_hierarchy_duplicates()
    print("\n📊 Final Report:")
    print(f"   Backup created at: {result['backup_created']}")
    print(f"   Validation passed: {result['validation_report']['validation_passed']}")
