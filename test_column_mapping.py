"""
Test script to demonstrate the new column mapping functionality.
"""

import pandas as pd
from tasks.csv_cleaning import standardize_headers


def test_column_mapping():
    """Test the column mapping functionality with sample data."""
    print("TESTING COLUMN MAPPING FUNCTIONALITY")
    print("=" * 60)

    # Load the sample health facilities data
    file_path = "data/raw/facilities/health/kayunga_health_facilities.csv"
    df = pd.read_csv(file_path, nrows=3)

    print(f"Original columns ({len(df.columns)}):")
    for i, col in enumerate(df.columns[:10]):  # Show first 10
        print(f"  {i+1:2d}. {col}")
    if len(df.columns) > 10:
        print(f"  ... and {len(df.columns) - 10} more columns")

    print("\n" + "-" * 60)

    # Apply the new column mapping
    df_mapped = standardize_headers(
        df,
        file_path=file_path,
        data_type="facility",
        thematic_area="health"
    )

    print(f"Mapped columns ({len(df_mapped.columns)}):")
    for i, col in enumerate(df_mapped.columns[:10]):  # Show first 10
        print(f"  {i+1:2d}. {col}")
    if len(df_mapped.columns) > 10:
        print(f"  ... and {len(df_mapped.columns) - 10} more columns")

    print("\n" + "-" * 60)
    print("COLUMN MAPPING EXAMPLES:")
    print("-" * 60)

    # Show some example mappings
    examples = [
        "District",
        "Subcounty/Towncouncil",
        "Name of the health facility/Clinic",
        "Healthy center level",
        "_Geo points_latitude",
        "_Geo points_longitude",
        "Immunization Services",
        "Outpatient Services e.g Treatment for common illnesses, minor injuries, and general health issues",
        "Is there at least one usable improved toilet designated for women and girls, which provides facilities to manage menstrual hygiene needs?"
    ]

    for original in examples:
        if original in df.columns:
            mapped = df_mapped.columns[df.columns.get_loc(original)]
            print(f"'{original}' → '{mapped}'")

    print("\n✓ Column mapping test completed!")
    return df_mapped


if __name__ == "__main__":
    test_column_mapping()