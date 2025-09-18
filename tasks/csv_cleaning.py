import re
from pathlib import Path

import pandas as pd
import yaml
from prefect import task
from slugify import slugify

# Type hints would be imported here if needed


@task(
    retries=3,
    cache_key_fn=lambda ctx, params: f"detect_data_type_{params['file_path']}",
)
def detect_data_type(
    file_path: str, config_path: str = "config/data_type_rules.yaml"
) -> str:
    """
    Detect if CSV contains facility data or aggregated data based on filename and content.

    Args:
        file_path: Path to the CSV file
        config_path: Path to configuration file

    Returns:
        'facility' or 'aggregated'
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    filename = Path(file_path).name.lower()

    # Check filename for facility indicators
    facility_indicators = config["facility_indicators"]
    aggregated_indicators = config["aggregated_indicators"]

    facility_score = sum(
        1 for indicator in facility_indicators if indicator in filename
    )
    aggregated_score = sum(
        1 for indicator in aggregated_indicators if indicator in filename
    )

    # Read file to check column structure
    df = pd.read_csv(file_path, nrows=5)
    columns = [col.lower() for col in df.columns]

    # Look for facility-specific columns
    has_facility_name = any(
        "facility" in col or "clinic" in col or "hospital" in col or "centre" in col
        for col in columns
    )
    has_coordinates = any("lat" in col or "geo" in col for col in columns)
    has_individual_records = len(df) > 10  # Facility data typically has many records

    # Scoring system
    if has_facility_name:
        facility_score += 3
    if has_coordinates:
        facility_score += 2
    if has_individual_records:
        facility_score += 1

    # Aggregated data indicators
    has_totals = any(
        "total" in col or "sum" in col or "count" in col for col in columns
    )
    has_percentages = any(
        "%" in str(val) for val in df.iloc[0].astype(str) if pd.notna(val)
    )

    if has_totals:
        aggregated_score += 2
    if has_percentages:
        aggregated_score += 2

    return "facility" if facility_score >= aggregated_score else "aggregated"


@task(
    retries=3,
    cache_key_fn=lambda ctx, params: f"detect_thematic_area_{params['file_path']}",
)
def detect_thematic_area(
    file_path: str, config_path: str = "config/data_type_rules.yaml"
) -> str:
    """
    Auto-identify thematic area from filename and content.

    Args:
        file_path: Path to the CSV file
        config_path: Path to configuration file

    Returns:
        Thematic area (health, education, water, demographics, economic, infrastructure)
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    filename = Path(file_path).name.lower()
    thematic_mapping = config["thematic_detection"]

    scores = {}
    for theme, indicators in thematic_mapping.items():
        score = sum(1 for indicator in indicators if indicator in filename)
        scores[theme] = score

    # If no clear winner from filename, check column headers
    if max(scores.values()) == 0:
        df = pd.read_csv(file_path, nrows=1)
        columns_text = " ".join(df.columns).lower()

        for theme, indicators in thematic_mapping.items():
            score = sum(1 for indicator in indicators if indicator in columns_text)
            scores[theme] += score

    return max(scores, key=scores.get) if max(scores.values()) > 0 else "general"


@task(retries=3)
def standardize_headers(
    df: pd.DataFrame,
    file_path: str = None,
    data_type: str = None,
    thematic_area: str = None,
    config_path: str = "config/column_mappings.yaml",
) -> pd.DataFrame:
    """
    Clean column names using configurable mappings.

    Args:
        df: Input DataFrame
        file_path: Original file path for pattern matching
        data_type: Data type (facility/aggregated) for specific mappings
        thematic_area: Thematic area for domain-specific mappings
        config_path: Path to column mappings configuration

    Returns:
        DataFrame with standardized column names
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    standardized_columns = {}

    for col in df.columns:
        original_col = str(col)
        new_col = None

        # 1. Check global mappings first
        if original_col in config.get("global_mappings", {}):
            new_col = config["global_mappings"][original_col]

        # 2. Check file pattern mappings
        elif file_path and "file_patterns" in config:
            filename = Path(file_path).name.lower()
            for pattern, mappings in config["file_patterns"].items():
                # Simple pattern matching (remove * and check if pattern is in filename)
                pattern_clean = pattern.replace("*", "")
                if pattern_clean in filename and original_col in mappings:
                    new_col = mappings[original_col]
                    break

        # 3. Check data type specific mappings
        elif data_type and data_type in config.get("data_type_mappings", {}):
            if original_col in config["data_type_mappings"][data_type]:
                new_col = config["data_type_mappings"][data_type][original_col]

        # 4. Check thematic area mappings
        elif thematic_area and thematic_area in config.get("thematic_mappings", {}):
            if original_col in config["thematic_mappings"][thematic_area]:
                new_col = config["thematic_mappings"][thematic_area][original_col]

        # 5. Apply fallback rules if no mapping found
        if new_col is None:
            new_col = _apply_fallback_rules(
                original_col, config.get("fallback_rules", {})
            )

        standardized_columns[original_col] = new_col

    return df.rename(columns=standardized_columns)


def _apply_fallback_rules(column_name: str, rules: dict) -> str:
    """
    Apply fallback rules to transform column names when no explicit mapping exists.

    Args:
        column_name: Original column name
        rules: Fallback rules configuration

    Returns:
        Transformed column name
    """
    result = str(column_name)

    # Remove prefixes
    for prefix in rules.get("remove_prefixes", []):
        if result.startswith(prefix):
            result = result[len(prefix) :]

    # Remove suffixes
    for suffix in rules.get("remove_suffixes", []):
        if result.endswith(suffix):
            result = result[: -len(suffix)]

    # Apply abbreviations
    for full_word, abbrev in rules.get("abbreviations", {}).items():
        result = result.replace(full_word, abbrev)

    # Convert to snake_case
    result = re.sub(r"[^\w\s]", "", result)
    result = re.sub(r"\s+", "_", result.strip())
    result = result.lower()

    # Clean up any double underscores
    result = re.sub(r"_+", "_", result).strip("_")

    return result if result else "unknown_column"


@task(retries=3)
def clean_location_data(
    df: pd.DataFrame, config_path: str = "config/location_mappings.yaml"
) -> pd.DataFrame:
    """
    Standardize location name formats and map variations.

    Args:
        df: Input DataFrame
        config_path: Path to location mappings configuration

    Returns:
        DataFrame with cleaned location data
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    location_cols = config["standard_location_columns"]
    df_cleaned = df.copy()

    # Identify location columns in the dataframe
    for level, possible_names in location_cols.items():
        matching_col = None
        for col in df.columns:
            if any(name.lower() in col.lower() for name in possible_names):
                matching_col = col
                break

        if matching_col:
            # Clean the location data
            df_cleaned[matching_col] = df_cleaned[matching_col].astype(str).str.strip()
            df_cleaned[matching_col] = df_cleaned[matching_col].str.title()

            # Apply specific mappings
            if level == "district" and "district_mappings" in config:
                for standard, variations in config["district_mappings"].items():
                    for variation in variations:
                        df_cleaned[matching_col] = df_cleaned[matching_col].replace(
                            variation, standard.title()
                        )

    return df_cleaned


@task(retries=3)
def generate_location_codes(
    df: pd.DataFrame, config_path: str = "config/data_type_rules.yaml"
) -> pd.DataFrame:
    """
    Generate location codes using format: UG.{DISTRICT}.{SUBCOUNTY}.{PARISH}.{VILLAGE}

    Args:
        df: Input DataFrame with location columns
        config_path: Path to configuration file

    Returns:
        DataFrame with location_code column added
    """
    # Load location mappings
    with open("config/location_mappings.yaml", "r") as f:
        location_config = yaml.safe_load(f)

    location_cols = location_config["standard_location_columns"]
    df_coded = df.copy()

    # Find location columns
    location_data = {}
    for level, possible_names in location_cols.items():
        for col in df.columns:
            if any(name.lower() in col.lower() for name in possible_names):
                location_data[level] = col
                break

    # Generate location codes
    location_codes = []
    for _, row in df.iterrows():
        code_parts = ["UG"]

        for level in ["district", "subcounty", "parish", "village"]:
            if level in location_data and pd.notna(row[location_data[level]]):
                slug = slugify(str(row[location_data[level]]), separator="_")
                code_parts.append(slug)
            else:
                code_parts.append("unknown")

        location_codes.append(".".join(code_parts))

    df_coded["location_code"] = location_codes
    return df_coded


@task(retries=3)
def generate_facility_ids(
    df: pd.DataFrame, config_path: str = "config/location_mappings.yaml"
) -> pd.DataFrame:
    """
    Generate facility IDs for facility data: {facility-name-slug}-{location-slug}

    Args:
        df: Input DataFrame with facility data
        config_path: Path to location mappings configuration

    Returns:
        DataFrame with facility_id column added
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    df_with_ids = df.copy()

    # Find facility name column
    facility_name_col = None
    facility_name_columns = config["facility_name_columns"]

    for col in df.columns:
        if any(
            name.lower() in col.lower()
            for name in [name.lower() for name in facility_name_columns]
        ):
            facility_name_col = col
            break

    if not facility_name_col:
        # Fallback: look for any column with 'name' in it
        for col in df.columns:
            if (
                "name" in col.lower()
                and "village" not in col.lower()
                and "parish" not in col.lower()
            ):
                facility_name_col = col
                break

    if facility_name_col:
        facility_ids = []
        for _, row in df.iterrows():
            facility_name = (
                str(row[facility_name_col])
                if pd.notna(row[facility_name_col])
                else "unknown"
            )
            facility_slug = slugify(facility_name, separator="_")

            # Create location slug from first available location
            location_parts = []
            if "location_code" in df.columns:
                location_slug = (
                    row["location_code"].replace("UG.", "").replace(".", "_")
                )
            else:
                # Fallback: use district and subcounty
                for col in df.columns:
                    if "district" in col.lower():
                        location_parts.append(slugify(str(row[col]), separator="_"))
                        break
                for col in df.columns:
                    if "subcounty" in col.lower() or "town" in col.lower():
                        location_parts.append(slugify(str(row[col]), separator="_"))
                        break
                location_slug = (
                    "_".join(location_parts) if location_parts else "unknown"
                )

            facility_id = f"{facility_slug}-{location_slug}"
            facility_ids.append(facility_id)

        df_with_ids["facility_id"] = facility_ids

    return df_with_ids
