import re
import json
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


def _format_camel_case(text: str) -> str:
    """
    Convert text to Camel Case format (first letter of each word capitalized).

    Args:
        text: Input text string

    Returns:
        Camel Case formatted text
    """
    if pd.isna(text) or not isinstance(text, str):
        return str(text)

    # Clean and format the text
    text = str(text).strip()

    # Handle special cases like abbreviations (keep them uppercase)
    words = text.split()
    formatted_words = []

    for word in words:
        # Keep abbreviations uppercase if they are 2-3 characters
        if len(word) <= 3 and word.isupper():
            formatted_words.append(word)
        else:
            formatted_words.append(word.capitalize())

    return " ".join(formatted_words)


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
    Standardize location name formats and map variations with Camel Case formatting.

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
            # Clean the location data with Camel Case
            df_cleaned[matching_col] = df_cleaned[matching_col].astype(str).str.strip()
            df_cleaned[matching_col] = df_cleaned[matching_col].apply(_format_camel_case)

            # Fix Towncouncil to Town Council
            df_cleaned[matching_col] = df_cleaned[matching_col].str.replace(
                "Towncouncil", "Town Council", regex=False
            )

            # Apply specific mappings
            if level == "district" and "district_mappings" in config:
                for standard, variations in config["district_mappings"].items():
                    for variation in variations:
                        df_cleaned[matching_col] = df_cleaned[matching_col].replace(
                            variation, _format_camel_case(standard)
                        )

    return df_cleaned


@task(retries=3)
def clean_nan_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace NaN values with appropriate defaults and fix staff count columns to be integers.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with cleaned NaN values and proper data types
    """
    df_cleaned = df.copy()

    # Replace all NaN values with empty strings for object/string columns
    for column in df_cleaned.columns:
        if df_cleaned[column].dtype == 'object':
            # Convert any remaining NaN/null values to empty strings
            df_cleaned[column] = df_cleaned[column].astype(str)
            df_cleaned[column] = df_cleaned[column].replace(['nan', 'NaN', 'Nan', 'None'], '')
            # Handle actual NaN values that might still exist
            df_cleaned[column] = df_cleaned[column].fillna('')

    # Fix staff count columns to be integers (not decimals)
    staff_columns = [
        'nursing_assistants', 'staff_comprehensive_nurse', 'enrolled_nurses',
        'staff_lab_assistant', 'laboratory_technician', 'medical_officers',
        'staff_clinical_officer', 'staff_midwives', 'staff_nursing_officers',
        'staff_vht_chw', 'staff_health_info'
    ]

    for column in staff_columns:
        if column in df_cleaned.columns:
            # Convert to numeric, replacing any non-numeric values with 0
            df_cleaned[column] = pd.to_numeric(df_cleaned[column], errors='coerce').fillna(0)
            # Convert to integer
            df_cleaned[column] = df_cleaned[column].astype(int)

    return df_cleaned


@task(retries=3)
def clean_facility_and_officer_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean facility names and officer in charge fields with Camel Case formatting.
    Exempts facility_level from Camel Case to preserve uppercase codes like HCII, HCIV.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with cleaned facility and officer data
    """
    df_cleaned = df.copy()

    # Clean facility name columns (but exclude facility_level)
    facility_name_patterns = [
        "facility_name", "name", "facility", "clinic", "hospital", "centre", "center"
    ]
    for col in df.columns:
        if any(pattern in col.lower() for pattern in facility_name_patterns):
            # Exclude location columns and facility_level from Camel Case
            if col.lower() not in ["village", "parish", "subcounty", "district", "facility_level"]:
                df_cleaned[col] = df_cleaned[col].astype(str).apply(_format_camel_case)

    # Clean officer in charge fields
    officer_patterns = ["officer_in_charge", "oic", "officer", "in_charge"]
    for col in df.columns:
        if any(pattern in col.lower() for pattern in officer_patterns):
            df_cleaned[col] = df_cleaned[col].astype(str).apply(_format_camel_case)

    return df_cleaned


@task(retries=3)
def generate_hierarchical_location_codes(
    df: pd.DataFrame, config_path: str = "config/location_mappings.yaml"
) -> pd.DataFrame:
    """
    Generate hierarchical location codes using format:
    - District: UG.KAY
    - Subcounty/Town Council: UG.KAY.KAN
    - Parish: UG.KAY.KAN.KAN
    - Village: UG.KAY.KAN.KAN.KIB

    Args:
        df: Input DataFrame with location columns
        config_path: Path to location mappings configuration

    Returns:
        DataFrame with location_code column added
    """
    with open(config_path, "r") as f:
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

    # Build location hierarchy and generate codes
    location_hierarchy = {}
    location_codes = []

    for _, row in df.iterrows():
        code_parts = ["UG"]

        # District level
        if "district" in location_data and pd.notna(row[location_data["district"]]):
            district = str(row[location_data["district"]])
            district_code = _generate_location_abbreviation(district, 3)
            code_parts.append(district_code)
        else:
            code_parts.append("UNK")

        # Subcounty level
        if "subcounty" in location_data and pd.notna(row[location_data["subcounty"]]):
            subcounty = str(row[location_data["subcounty"]])
            subcounty_code = _generate_location_abbreviation(subcounty, 3)
            code_parts.append(subcounty_code)
        else:
            code_parts.append("UNK")

        # Parish level
        if "parish" in location_data and pd.notna(row[location_data["parish"]]):
            parish = str(row[location_data["parish"]])
            parish_code = _generate_location_abbreviation(parish, 3)
            code_parts.append(parish_code)
        else:
            code_parts.append("UNK")

        # Village level
        if "village" in location_data and pd.notna(row[location_data["village"]]):
            village = str(row[location_data["village"]])
            village_code = _generate_location_abbreviation(village, 3)
            code_parts.append(village_code)
        else:
            code_parts.append("UNK")

        location_codes.append(".".join(code_parts))

    df_coded["location_code"] = location_codes
    return df_coded


def _generate_location_abbreviation(name: str, max_length: int = 3) -> str:
    """
    Generate location abbreviation from name.

    Args:
        name: Location name
        max_length: Maximum length of abbreviation

    Returns:
        Abbreviated location code
    """
    if not name or pd.isna(name):
        return "UNK"

    # Clean the name
    clean_name = str(name).strip().upper()

    # Remove common words
    stop_words = ["TOWN", "COUNCIL", "SUBCOUNTY", "SUB", "COUNTY"]
    words = clean_name.split()
    meaningful_words = [w for w in words if w not in stop_words]

    if not meaningful_words:
        meaningful_words = words

    # Generate abbreviation
    if len(meaningful_words) == 1:
        # Single word - take first max_length characters
        return meaningful_words[0][:max_length]
    else:
        # Multiple words - take first letter of each word
        abbrev = "".join([w[0] for w in meaningful_words if w])
        if len(abbrev) <= max_length:
            return abbrev
        else:
            # If too long, take first max_length characters of first word
            return meaningful_words[0][:max_length]


@task(retries=3)
def generate_location_hierarchy_file(
    df: pd.DataFrame,
    output_path: str = "data/processed/locations/location_hierarchy.json",
    config_path: str = "config/location_mappings.yaml"
) -> dict:
    """
    Generate location hierarchy JSON file in the specified format.

    Args:
        df: Input DataFrame with location data
        output_path: Path to save the JSON file
        config_path: Path to location mappings configuration

    Returns:
        Dictionary containing the location hierarchy
    """
    with open(config_path, "r") as f:
        location_config = yaml.safe_load(f)

    location_cols = location_config["standard_location_columns"]

    # Find location columns
    location_data = {}
    for level, possible_names in location_cols.items():
        for col in df.columns:
            if any(name.lower() in col.lower() for name in possible_names):
                location_data[level] = col
                break

    # Build hierarchy structure
    hierarchy = {"districts": []}
    districts_map = {}

    for _, row in df.iterrows():
        # Get location values
        district = str(row[location_data["district"]]) if "district" in location_data and pd.notna(row[location_data["district"]]) else None
        subcounty = str(row[location_data["subcounty"]]) if "subcounty" in location_data and pd.notna(row[location_data["subcounty"]]) else None
        parish = str(row[location_data["parish"]]) if "parish" in location_data and pd.notna(row[location_data["parish"]]) else None
        village = str(row[location_data["village"]]) if "village" in location_data and pd.notna(row[location_data["village"]]) else None

        if not district:
            continue

        # Process district
        district_id = slugify(district, separator="-")
        district_code = f"UG.{_generate_location_abbreviation(district, 3)}"

        if district_id not in districts_map:
            districts_map[district_id] = {
                "id": district_id,
                "name": district,
                "code": district_code,
                "subcounties": []
            }
            hierarchy["districts"].append(districts_map[district_id])

        district_obj = districts_map[district_id]

        if not subcounty:
            continue

        # Process subcounty
        subcounty_id = slugify(subcounty, separator="-")
        subcounty_code = f"{district_code}.{_generate_location_abbreviation(subcounty, 3)}"

        # Find or create subcounty
        subcounty_obj = None
        for sc in district_obj["subcounties"]:
            if sc["id"] == subcounty_id:
                subcounty_obj = sc
                break

        if not subcounty_obj:
            subcounty_obj = {
                "id": subcounty_id,
                "name": subcounty,
                "code": subcounty_code,
                "parishes": []
            }
            district_obj["subcounties"].append(subcounty_obj)

        if not parish:
            continue

        # Process parish
        parish_id = slugify(parish, separator="-")
        parish_code = f"{subcounty_code}.{_generate_location_abbreviation(parish, 3)}"

        # Find or create parish
        parish_obj = None
        for p in subcounty_obj["parishes"]:
            if p["id"] == parish_id:
                parish_obj = p
                break

        if not parish_obj:
            parish_obj = {
                "id": parish_id,
                "name": parish,
                "code": parish_code,
                "villages": []
            }
            subcounty_obj["parishes"].append(parish_obj)

        if not village:
            continue

        # Process village
        village_id = slugify(village, separator="-")
        village_code = f"{parish_code}.{_generate_location_abbreviation(village, 3)}"

        # Check if village already exists
        village_exists = any(v["id"] == village_id for v in parish_obj["villages"])

        if not village_exists:
            village_obj = {
                "id": village_id,
                "name": village,
                "code": village_code
            }
            parish_obj["villages"].append(village_obj)

    # Save to file
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(hierarchy, f, indent=2, ensure_ascii=False)

    return hierarchy


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
