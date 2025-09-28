import json
from pathlib import Path
from typing import Dict, List

import pandas as pd
import yaml
from prefect import task
from slugify import slugify


@task(retries=3)
def extract_all_locations(
    df: pd.DataFrame, config_path: str = "config/location_mappings.yaml"
) -> Dict:
    """
    Extract comprehensive location list from dataset.

    Args:
        df: Input DataFrame
        config_path: Path to location mappings configuration

    Returns:
        Dictionary with location hierarchy
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    location_cols = config["standard_location_columns"]
    locations = {
        "districts": set(),
        "subcounties": set(),
        "parishes": set(),
        "villages": set(),
        "hierarchy": [],
    }

    # Find location columns
    col_mapping = {}
    for level, possible_names in location_cols.items():
        for col in df.columns:
            if any(name.lower() in col.lower() for name in possible_names):
                col_mapping[level] = col
                break

    # Extract unique locations
    for _, row in df.iterrows():
        location_record = {}
        for level in ["district", "subcounty", "parish", "village"]:
            if level in col_mapping and pd.notna(row[col_mapping[level]]):
                value = str(row[col_mapping[level]]).strip()
                # Map to correct plural forms
                plural_mapping = {
                    "district": "districts",
                    "subcounty": "subcounties",
                    "parish": "parishes",
                    "village": "villages",
                }
                level_key = plural_mapping[level]
                locations[level_key].add(value)
                location_record[level] = value

        if location_record:
            locations["hierarchy"].append(location_record)

    # Convert sets to sorted lists for JSON serialization
    for key in ["districts", "subcounties", "parishes", "villages"]:
        locations[key] = sorted(list(locations[key]))

    return locations


@task(retries=3)
def resolve_location_conflicts(
    locations_dict: Dict, existing_locations_path: str = None
) -> Dict:
    """
    Handle location name variations and conflicts.

    Args:
        locations_dict: Dictionary with extracted locations
        existing_locations_path: Path to existing master locations file

    Returns:
        Dictionary with resolved location conflicts
    """
    resolved = locations_dict.copy()

    # Load existing locations if available (placeholder for future enhancement)

    # Define common variations and their standard forms
    variations = {
        "towncouncil": ["town council", "tc", "town_council"],
        "subcounty": ["sub county", "sub_county", "sub-county"],
        "health centre": ["health center", "hc", "health_centre"],
    }

    # Resolve variations for each level
    for level in ["districts", "subcounties", "parishes", "villages"]:
        standardized = set()
        for location in resolved[level]:
            location_lower = location.lower()
            standard_form = location

            # Check against known variations
            for standard, vars_list in variations.items():
                for var in vars_list:
                    if var in location_lower:
                        standard_form = location_lower.replace(var, standard)
                        break

            standardized.add(standard_form.title())

        resolved[level] = sorted(list(standardized))

    return resolved


@task(retries=3)
def generate_unified_location_codes(locations_dict: Dict) -> Dict:
    """
    Generate unique location codes across districts.

    Args:
        locations_dict: Dictionary with location hierarchy

    Returns:
        Dictionary with location codes added
    """
    coded_locations = locations_dict.copy()
    coded_locations["location_codes"] = {}

    # Generate codes for each complete hierarchy
    for record in locations_dict["hierarchy"]:
        if "district" in record:
            code_parts = ["UG"]

            for level in ["district", "subcounty", "parish", "village"]:
                if level in record:
                    slug = slugify(record[level], separator="_")
                    code_parts.append(slug)
                else:
                    code_parts.append("unknown")

            location_code = ".".join(code_parts)
            coded_locations["location_codes"][location_code] = record

    return coded_locations


@task(retries=3)
def update_master_locations(
    new_locations: Dict,
    master_file_path: str = "data/processed/locations/master_locations.json",
) -> Dict:
    """
    Merge new locations with existing master location data.

    Args:
        new_locations: Dictionary with new location data
        master_file_path: Path to master locations file

    Returns:
        Updated master locations dictionary
    """
    master_path = Path(master_file_path)
    master_path.parent.mkdir(parents=True, exist_ok=True)

    # Load existing master locations if available
    master_locations = {
        "districts": [],
        "subcounties": [],
        "parishes": [],
        "villages": [],
        "hierarchy": [],
        "location_codes": {},
        "last_updated": None,
    }

    if master_path.exists():
        with open(master_path, "r") as f:
            master_locations = json.load(f)

    # Merge new locations
    for level in ["districts", "subcounties", "parishes", "villages"]:
        existing_set = set(master_locations[level])
        new_set = set(new_locations[level])
        combined = sorted(list(existing_set.union(new_set)))
        master_locations[level] = combined

    # Merge hierarchy records
    existing_hierarchy = {
        json.dumps(record, sort_keys=True) for record in master_locations["hierarchy"]
    }
    new_hierarchy = {
        json.dumps(record, sort_keys=True) for record in new_locations["hierarchy"]
    }
    combined_hierarchy = existing_hierarchy.union(new_hierarchy)
    master_locations["hierarchy"] = [
        json.loads(record) for record in combined_hierarchy
    ]

    # Merge location codes
    master_locations["location_codes"].update(new_locations.get("location_codes", {}))

    # Update timestamp
    from datetime import datetime

    master_locations["last_updated"] = datetime.now().isoformat()

    # Save updated master locations
    with open(master_path, "w") as f:
        json.dump(master_locations, f, indent=2)

    return master_locations


@task(retries=3)
def validate_location_hierarchy(
    df: pd.DataFrame,
    master_locations_path: str = "data/processed/locations/locations_hierarchy.json",
) -> Dict:
    """
    Validate location hierarchy consistency against master locations.

    Args:
        df: Input DataFrame
        master_locations_path: Path to master locations file

    Returns:
        Validation report dictionary
    """
    validation_report = {
        "valid_records": 0,
        "invalid_records": 0,
        "missing_locations": {},
        "hierarchy_conflicts": [],
        "recommendations": [],
    }

    # Load master locations if available
    master_locations = {}
    if Path(master_locations_path).exists():
        with open(master_locations_path, "r") as f:
            master_locations = json.load(f)

    # Load location mappings config
    with open("config/location_mappings.yaml", "r") as f:
        config = yaml.safe_load(f)

    location_cols = config["standard_location_columns"]

    # Find location columns in dataframe
    col_mapping = {}
    for level, possible_names in location_cols.items():
        for col in df.columns:
            if any(name.lower() in col.lower() for name in possible_names):
                col_mapping[level] = col
                break

    # Validate each record
    for idx, row in df.iterrows():
        record_valid = True
        location_record = {}

        for level in ["district", "subcounty", "parish", "village"]:
            if level in col_mapping and pd.notna(row[col_mapping[level]]):
                value = str(row[col_mapping[level]]).strip()
                location_record[level] = value

                # Check against master locations
                plural_mapping = {
                    "district": "districts",
                    "subcounty": "subcounties",
                    "parish": "parishes",
                    "village": "villages",
                }
                level_key = plural_mapping[level]
                if master_locations and level_key in master_locations:
                    if value not in master_locations[level_key]:
                        record_valid = False
                        if level not in validation_report["missing_locations"]:
                            validation_report["missing_locations"][level] = set()
                        validation_report["missing_locations"][level].add(value)

        if record_valid:
            validation_report["valid_records"] += 1
        else:
            validation_report["invalid_records"] += 1

    # Convert sets to lists for JSON serialization
    for level in validation_report["missing_locations"]:
        validation_report["missing_locations"][level] = list(
            validation_report["missing_locations"][level]
        )

    # Generate recommendations
    if validation_report["invalid_records"] > 0:
        validation_report["recommendations"].append(
            "Review missing locations and update master locations file"
        )
        validation_report["recommendations"].append(
            "Consider standardizing location name variations"
        )

    return validation_report


@task(retries=3)
def cross_reference_locations(
    df: pd.DataFrame, reference_datasets: List[str] = None
) -> Dict:
    """
    Cross-reference locations against other known datasets.

    Args:
        df: Input DataFrame
        reference_datasets: List of paths to reference datasets

    Returns:
        Cross-reference report
    """
    report = {
        "matched_locations": 0,
        "unmatched_locations": [],
        "reference_sources": reference_datasets or [],
        "confidence_scores": {},
    }

    # This is a placeholder for cross-referencing logic
    # In a full implementation, this would compare against
    # official district databases, census data, etc.

    # For now, perform basic validation
    # Validate against known districts (this should come from reference data)
    # Find district column
    district_col = None
    for col in df.columns:
        if any("district" in col.lower() for col in [col]):
            district_col = col
            break

    if district_col:
        unique_districts = df[district_col].dropna().unique()
        known_districts = ["Kayunga"]  # This should come from reference data

        for district in unique_districts:
            if district in known_districts:
                report["matched_locations"] += 1
            else:
                report["unmatched_locations"].append(district)

    return report
