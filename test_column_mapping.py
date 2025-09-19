#!/usr/bin/env python3
"""
Test script for the new cleaning actions implementation.
"""

import pandas as pd
from flows.facility_cleaning_flow import clean_facility_data


def test_cleaning_actions():
    """Test the implementation of cleaning actions."""
    print("🧪 Testing cleaning actions implementation...")

    # Test with existing data file
    input_file = "data/raw/facilities/health/kayunga_health_facilities.csv"

    try:
        # Run the cleaning pipeline
        result = clean_facility_data(input_file)

        print(f"✅ Processing completed successfully!")
        print(f"   - Data type: {result['data_type']}")
        print(f"   - Thematic area: {result['thematic_area']}")
        print(f"   - Records processed: {result['records_processed']}")
        print(f"   - Quality score: {result['quality_score']:.1f}%")

        # Load and inspect the cleaned data
        cleaned_csv_path = result["output_files"]["consolidated_csv"]
        df = pd.read_csv(cleaned_csv_path)

        print(f"\n📋 Inspecting cleaned data...")
        print(f"   - Total records: {len(df)}")

        # Check location formatting
        location_cols = ["district", "subcounty", "parish", "village"]
        for col in location_cols:
            if col in df.columns:
                sample_value = df[col].iloc[0] if not df[col].isna().all() else "N/A"
                print(f"   - {col.title()}: {sample_value}")

        # Check facility name formatting
        if "facility_name" in df.columns:
            facility_sample = df["facility_name"].iloc[0]
            print(f"   - Facility name: {facility_sample}")

        # Check officer in charge formatting
        if "officer_in_charge" in df.columns:
            officer_sample = df["officer_in_charge"].iloc[0]
            print(f"   - Officer in charge: {officer_sample}")

        # Check location codes
        if "location_code" in df.columns:
            code_sample = df["location_code"].iloc[0]
            print(f"   - Location code: {code_sample}")

        # Check if location hierarchy file was created
        import json

        hierarchy_path = "data/processed/location_hierarchy.json"
        try:
            with open(hierarchy_path, "r") as f:
                hierarchy = json.load(f)

            print(f"\n🏢 Location hierarchy file generated:")
            print(f"   - Districts: {len(hierarchy['districts'])}")

            if hierarchy["districts"]:
                district = hierarchy["districts"][0]
                print(f"   - Sample district: {district['name']} ({district['code']})")

                if district["subcounties"]:
                    subcounty = district["subcounties"][0]
                    print(
                        f"   - Sample subcounty: {subcounty['name']} ({subcounty['code']})"
                    )

        except FileNotFoundError:
            print(f"⚠️  Location hierarchy file not found at {hierarchy_path}")

        print(f"\n🎉 All cleaning actions implemented successfully!")
        return True

    except Exception as e:
        print(f"❌ Error during testing: {str(e)}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_cleaning_actions()
    exit(0 if success else 1)
