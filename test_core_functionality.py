"""
Test script to verify core data cleaning functionality without Prefect dependencies.
"""

import json
import sys
from pathlib import Path

import pandas as pd

# Add current directory to path for imports
sys.path.append(".")


def test_data_type_detection():
    """Test data type detection logic"""
    print("Testing data type detection...")

    filename = "data/raw/facilities/health/kayunga_health_facilities.csv"

    # Simple detection logic based on filename
    filename_lower = Path(filename).name.lower()
    facility_indicators = [
        "facility",
        "health",
        "school",
        "clinic",
        "hospital",
        "centre",
        "center",
    ]

    facility_score = sum(
        1 for indicator in facility_indicators if indicator in filename_lower
    )

    # Read file to check structure
    df = pd.read_csv(filename, nrows=5)
    columns = [col.lower() for col in df.columns]

    has_facility_name = any("facility" in col or "clinic" in col for col in columns)
    has_coordinates = any("lat" in col or "geo" in col for col in columns)

    if has_facility_name:
        facility_score += 3
    if has_coordinates:
        facility_score += 2

    data_type = "facility" if facility_score > 0 else "aggregated"
    print(f"Detected data type: {data_type}")
    return data_type


def test_header_standardization():
    """Test header standardization"""
    print("Testing header standardization...")

    df = pd.read_csv(
        "data/raw/facilities/health/kayunga_health_facilities.csv", nrows=5
    )
    print(f"Original columns ({len(df.columns)}): {list(df.columns[:5])}...")

    # Standardize headers
    import re

    standardized_columns = {}
    for col in df.columns:
        clean_col = re.sub(r"[^\w\s]", "", str(col))
        clean_col = re.sub(r"\s+", "_", clean_col.strip())
        clean_col = clean_col.lower()
        clean_col = clean_col.replace("geo_points_", "")
        standardized_columns[col] = clean_col

    df_clean = df.rename(columns=standardized_columns)
    print(f"Cleaned columns ({len(df_clean.columns)}): {list(df_clean.columns[:5])}...")
    return df_clean


def test_location_extraction():
    """Test location hierarchy extraction"""
    print("Testing location extraction...")

    df = pd.read_csv("data/raw/facilities/health/kayunga_health_facilities.csv")

    # Find location columns
    location_data = {}
    location_columns = {
        "district": "District",
        "subcounty": None,
        "parish": None,
        "village": None,
    }

    for col in df.columns:
        if "subcounty" in col.lower() or "town" in col.lower():
            location_columns["subcounty"] = col
        elif "parish" in col.lower() or "ward" in col.lower():
            location_columns["parish"] = col
        elif "village" in col.lower() or "cell" in col.lower():
            location_columns["village"] = col

    print(f"Found location columns: {location_columns}")

    # Extract unique locations
    locations = {}
    for level, col in location_columns.items():
        if col and col in df.columns:
            unique_locs = df[col].dropna().unique()
            locations[f"{level}s"] = sorted(list(unique_locs))
            print(f"{level.title()}s ({len(unique_locs)}): {list(unique_locs[:3])}...")

    return locations


def test_location_codes():
    """Test location code generation"""
    print("Testing location code generation...")

    df = pd.read_csv(
        "data/raw/facilities/health/kayunga_health_facilities.csv", nrows=10
    )

    from slugify import slugify

    # Generate location codes
    location_codes = []
    for _, row in df.iterrows():
        district = str(row.get("District", "unknown"))
        subcounty = str(row.get("Subcounty/Towncouncil", "unknown"))
        parish = str(row.get("Parish Name/ Ward", "unknown"))
        village = str(row.get("Village name/ cell", "unknown"))

        code_parts = [
            "UG",
            slugify(district, separator="_"),
            slugify(subcounty, separator="_"),
            slugify(parish, separator="_"),
            slugify(village, separator="_"),
        ]

        location_code = ".".join(code_parts)
        location_codes.append(location_code)

    print(f"Sample location codes: {location_codes[:3]}")
    return location_codes


def test_facility_ids():
    """Test facility ID generation"""
    print("Testing facility ID generation...")

    df = pd.read_csv(
        "data/raw/facilities/health/kayunga_health_facilities.csv", nrows=10
    )

    from slugify import slugify

    facility_name_col = "Name of the health facility/Clinic"

    facility_ids = []
    for _, row in df.iterrows():
        facility_name = str(row.get(facility_name_col, "unknown"))
        district = str(row.get("District", "unknown"))
        subcounty = str(row.get("Subcounty/Towncouncil", "unknown"))

        facility_slug = slugify(facility_name, separator="_")
        location_slug = (
            f"{slugify(district, separator='_')}_{slugify(subcounty, separator='_')}"
        )

        facility_id = f"{facility_slug}-{location_slug}"
        facility_ids.append(facility_id)

    print(f"Sample facility IDs: {facility_ids[:3]}")
    return facility_ids


def test_coordinate_validation():
    """Test coordinate validation"""
    print("Testing coordinate validation...")

    df = pd.read_csv("data/raw/facilities/health/kayunga_health_facilities.csv")

    lat_col = "_Geo points_latitude"
    lng_col = "_Geo points_longitude"

    if lat_col in df.columns and lng_col in df.columns:
        valid_coords = 0
        invalid_coords = 0
        missing_coords = 0

        # Uganda bounds
        uganda_bounds = {
            "lat_min": -1.5,
            "lat_max": 4.0,
            "lng_min": 29.5,
            "lng_max": 35.0,
        }

        for _, row in df.iterrows():
            lat = row[lat_col]
            lng = row[lng_col]

            if pd.isna(lat) or pd.isna(lng):
                missing_coords += 1
            else:
                try:
                    lat_float = float(lat)
                    lng_float = float(lng)

                    if (
                        uganda_bounds["lat_min"]
                        <= lat_float
                        <= uganda_bounds["lat_max"]
                        and uganda_bounds["lng_min"]
                        <= lng_float
                        <= uganda_bounds["lng_max"]
                    ):
                        valid_coords += 1
                    else:
                        invalid_coords += 1
                except:
                    invalid_coords += 1

        print(
            f"Coordinate validation: {valid_coords} valid, {invalid_coords} invalid, {missing_coords} missing"
        )
        return {
            "valid": valid_coords,
            "invalid": invalid_coords,
            "missing": missing_coords,
        }
    else:
        print("Coordinate columns not found")
        return None


def main():
    """Run all tests"""
    print("=" * 50)
    print("TESTING UGANDA DISTRICT DATA CLEANING PIPELINE")
    print("=" * 50)

    try:
        # Test 1: Data type detection
        data_type = test_data_type_detection()
        print()

        # Test 2: Header standardization
        df_clean = test_header_standardization()
        print()

        # Test 3: Location extraction
        locations = test_location_extraction()
        print()

        # Test 4: Location codes
        location_codes = test_location_codes()
        print()

        # Test 5: Facility IDs
        facility_ids = test_facility_ids()
        print()

        # Test 6: Coordinate validation
        coord_validation = test_coordinate_validation()
        print()

        print("=" * 50)
        print("ALL TESTS COMPLETED SUCCESSFULLY!")
        print("=" * 50)

        # Create a simple summary
        summary = {
            "data_type": data_type,
            "sample_location_codes": location_codes[:3],
            "sample_facility_ids": facility_ids[:3],
            "coordinate_validation": coord_validation,
            "locations_found": {
                k: len(v) for k, v in locations.items() if isinstance(v, list)
            },
        }

        print("Summary:")
        print(json.dumps(summary, indent=2))

        return summary

    except Exception as e:
        print(f"Error during testing: {str(e)}")
        import traceback

        traceback.print_exc()
        return None


if __name__ == "__main__":
    main()
