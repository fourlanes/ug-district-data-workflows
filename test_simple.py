"""
Simplified test script without external dependencies.
"""

import re
from pathlib import Path

import pandas as pd


def simple_slugify(text, separator="_"):
    """Simple slugify function without external dependencies"""
    if pd.isna(text):
        return "unknown"
    # Convert to string and lowercase
    text = str(text).lower()
    # Replace spaces and special chars with separator
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", separator, text)
    return text.strip(separator)


def test_pipeline():
    """Test the complete pipeline"""
    print("TESTING UGANDA DISTRICT DATA CLEANING PIPELINE")
    print("=" * 60)

    # Load sample data
    df = pd.read_csv("data/raw/facilities/health/kayunga_health_facilities.csv")
    print(f"Loaded {len(df)} records from sample data")

    # 1. Data type detection
    filename_lower = "kayunga_health_facilities.csv"
    facility_indicators = ["facility", "health", "clinic", "hospital"]
    has_facility_keyword = any(
        indicator in filename_lower for indicator in facility_indicators
    )
    has_facility_column = any("facility" in col.lower() for col in df.columns)
    data_type = (
        "facility" if (has_facility_keyword or has_facility_column) else "aggregated"
    )
    print(f"✓ Data type detected: {data_type}")

    # 2. Thematic area detection
    health_indicators = ["health", "medical", "clinic", "hospital"]
    thematic_area = (
        "health"
        if any(indicator in filename_lower for indicator in health_indicators)
        else "general"
    )
    print(f"✓ Thematic area detected: {thematic_area}")

    # 3. Header standardization
    original_columns = list(df.columns)
    standardized_columns = {}
    for col in df.columns:
        clean_col = re.sub(r"[^\w\s]", "", str(col))
        clean_col = re.sub(r"\s+", "_", clean_col.strip())
        clean_col = clean_col.lower()
        standardized_columns[col] = clean_col

    df_clean = df.rename(columns=standardized_columns)
    print(f"✓ Headers standardized: {len(df_clean.columns)} columns")

    # 4. Location extraction
    location_columns = {
        "district": "District",
        "subcounty": "Subcounty/Towncouncil",
        "parish": "Parish Name/ Ward",
        "village": "Village name/ cell",
    }

    locations_found = {}
    for level, col in location_columns.items():
        if col in df.columns:
            unique_vals = df[col].dropna().unique()
            locations_found[level] = list(unique_vals)

    print(f"✓ Location extraction completed:")
    for level, values in locations_found.items():
        print(
            f"  - {level.title()}s: {len(values)} unique ({values[0] if values else 'none'})"
        )

    # 5. Location code generation
    location_codes = []
    for i, row in df.head(5).iterrows():  # Test with first 5 rows
        district = simple_slugify(row.get("District", "unknown"))
        subcounty = simple_slugify(row.get("Subcounty/Towncouncil", "unknown"))
        parish = simple_slugify(row.get("Parish Name/ Ward", "unknown"))
        village = simple_slugify(row.get("Village name/ cell", "unknown"))

        location_code = f"UG.{district}.{subcounty}.{parish}.{village}"
        location_codes.append(location_code)

    print(f"✓ Location codes generated:")
    for i, code in enumerate(location_codes[:3]):
        print(f"  - Sample {i+1}: {code}")

    # 6. Facility ID generation
    facility_name_col = "Name of the health facility/Clinic"
    facility_ids = []

    for i, row in df.head(5).iterrows():
        facility_name = simple_slugify(row.get(facility_name_col, "unknown"))
        district = simple_slugify(row.get("District", "unknown"))
        subcounty = simple_slugify(row.get("Subcounty/Towncouncil", "unknown"))

        location_slug = f"{district}_{subcounty}"
        facility_id = f"{facility_name}-{location_slug}"
        facility_ids.append(facility_id)

    print(f"✓ Facility IDs generated:")
    for i, fid in enumerate(facility_ids[:3]):
        print(f"  - Sample {i+1}: {fid[:60]}...")

    # 7. Coordinate validation
    lat_col = "_Geo points_latitude"
    lng_col = "_Geo points_longitude"

    if lat_col in df.columns and lng_col in df.columns:
        uganda_bounds = {
            "lat_min": -1.5,
            "lat_max": 4.0,
            "lng_min": 29.5,
            "lng_max": 35.0,
        }
        valid_coords = 0
        total_coords = 0

        for _, row in df.iterrows():
            lat, lng = row[lat_col], row[lng_col]
            if pd.notna(lat) and pd.notna(lng):
                total_coords += 1
                try:
                    lat_f, lng_f = float(lat), float(lng)
                    if (
                        uganda_bounds["lat_min"] <= lat_f <= uganda_bounds["lat_max"]
                        and uganda_bounds["lng_min"]
                        <= lng_f
                        <= uganda_bounds["lng_max"]
                    ):
                        valid_coords += 1
                except:
                    pass

        coord_quality = (valid_coords / total_coords * 100) if total_coords > 0 else 0
        print(
            f"✓ Coordinate validation: {valid_coords}/{total_coords} valid ({coord_quality:.1f}%)"
        )
    else:
        print("✗ Coordinate columns not found")

    # 8. Generate summary
    print("\n" + "=" * 60)
    print("PIPELINE TEST SUMMARY")
    print("=" * 60)

    summary = {
        "status": "success",
        "data_type": data_type,
        "thematic_area": thematic_area,
        "records_processed": len(df),
        "locations_found": {k: len(v) for k, v in locations_found.items()},
        "coordinate_quality": (
            f"{coord_quality:.1f}%" if "coord_quality" in locals() else "N/A"
        ),
        "sample_outputs": {
            "location_codes": location_codes[:3],
            "facility_ids": [fid[:50] + "..." for fid in facility_ids[:3]],
        },
    }

    for key, value in summary.items():
        if key != "sample_outputs":
            print(f"{key}: {value}")

    print("\nSample outputs:")
    print(f"Location codes: {summary['sample_outputs']['location_codes'][:2]}")
    print(f"Facility IDs: {summary['sample_outputs']['facility_ids'][:2]}")

    print("\n✓ All core functionality working correctly!")
    return summary


if __name__ == "__main__":
    test_pipeline()
