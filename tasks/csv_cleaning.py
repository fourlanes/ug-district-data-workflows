import json
import re
from pathlib import Path

import pandas as pd
import yaml
from prefect import task
from slugify import slugify

# Type hints would be imported here if needed


def deduplicate_location_name(name: str, level: str) -> str:
    """
    Deduplicate location names by removing qualifiers like 'Subcounty' and 'Parish'
    where they conflict with base names.

    Args:
        name: The location name to deduplicate
        level: The location level ('subcounty' or 'parish')

    Returns:
        Deduplicated name (prefers base name over qualified name)
    """
    if not name or not isinstance(name, str):
        return name

    name_stripped = name.strip()

    # Only apply deduplication for subcounty and parish levels
    if level == "subcounty":
        # Remove "Subcounty" qualifier if present
        if name_stripped.endswith(" Subcounty"):
            return name_stripped[:-10]  # Remove " Subcounty"
    elif level == "parish":
        # Remove "Parish" qualifier if present
        if name_stripped.endswith(" Parish"):
            return name_stripped[:-7]  # Remove " Parish"

    return name_stripped


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
    df = pd.read_csv(file_path, nrows=5, encoding="utf-8-sig")
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
        df = pd.read_csv(file_path, nrows=1, encoding="utf-8-sig")
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
            df_cleaned[matching_col] = df_cleaned[matching_col].apply(
                _format_camel_case
            )

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
        if df_cleaned[column].dtype == "object":
            # Convert any remaining NaN/null values to empty strings
            df_cleaned[column] = df_cleaned[column].astype(str)
            df_cleaned[column] = df_cleaned[column].replace(
                ["nan", "NaN", "Nan", "None"], ""
            )
            # Handle actual NaN values that might still exist
            df_cleaned[column] = df_cleaned[column].fillna("")

    # Fix staff count columns to be integers (not decimals)
    staff_columns = [
        "nursing_assistants",
        "staff_comprehensive_nurse",
        "enrolled_nurses",
        "staff_lab_assistant",
        "laboratory_technician",
        "medical_officers",
        "staff_clinical_officer",
        "staff_midwives",
        "staff_nursing_officers",
        "staff_vht_chw",
        "staff_health_info",
    ]

    for column in staff_columns:
        if column in df_cleaned.columns:
            # Convert to numeric, replacing any non-numeric values with 0
            df_cleaned[column] = pd.to_numeric(
                df_cleaned[column], errors="coerce"
            ).fillna(0)
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
        "facility_name",
        "name",
        "facility",
        "clinic",
        "hospital",
        "centre",
        "center",
    ]
    for col in df.columns:
        if any(pattern in col.lower() for pattern in facility_name_patterns):
            # Exclude location columns and facility_level from Camel Case
            if col.lower() not in [
                "village",
                "parish",
                "subcounty",
                "district",
                "facility_level",
            ]:
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
    Generate hierarchical location codes by looking up from the location hierarchy file.
    Uses flexible location matching with hierarchical fallback (village → parish → subcounty → district).
    This ensures consistency between hierarchy and facility codes.

    Args:
        df: Input DataFrame with location columns
        config_path: Path to location mappings configuration

    Returns:
        DataFrame with location_code column added
    """
    # Try to load existing hierarchy first
    hierarchy_file = "data/processed/locations/location_hierarchy.json"
    location_lookup = {}

    try:
        with open(hierarchy_file, "r") as f:
            hierarchy = json.load(f)

        # Build comprehensive lookup table for all location levels
        for district in hierarchy.get("districts", []):
            # District level
            district_key = (district["name"], "", "", "")
            location_lookup[district_key] = district["code"]

            for subcounty in district.get("subcounties", []):
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

    except (FileNotFoundError, json.JSONDecodeError):
        # Fall back to generating codes if hierarchy doesn't exist
        return _generate_codes_from_scratch(df, config_path)

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

    location_codes = []

    for _, row in df.iterrows():
        # Get location values and clean them
        district = (
            str(row.get(location_data.get("district", ""), "")).strip()
            if location_data.get("district")
            else ""
        )
        subcounty = (
            str(row.get(location_data.get("subcounty", ""), "")).strip()
            if location_data.get("subcounty")
            else ""
        )
        parish = (
            str(row.get(location_data.get("parish", ""), "")).strip()
            if location_data.get("parish")
            else ""
        )
        village = (
            str(row.get(location_data.get("village", ""), "")).strip()
            if location_data.get("village")
            else ""
        )

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

    df_coded["location_code"] = location_codes
    return df_coded


def _generate_codes_from_scratch(df: pd.DataFrame, config_path: str) -> pd.DataFrame:
    """
    Fallback method to generate codes when hierarchy file doesn't exist.
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

    # Initialize GLOBAL collision tracking for ALL location codes
    global_used_codes = set()
    location_codes = []

    for _, row in df.iterrows():
        code_parts = ["UG"]

        # District level
        if "district" in location_data and pd.notna(row[location_data["district"]]):
            district = str(row[location_data["district"]])
            district_code = _generate_location_abbreviation(
                district, 3, global_used_codes
            )
            code_parts.append(district_code)
        else:
            code_parts.append("UNK")

        # Subcounty level
        if "subcounty" in location_data and pd.notna(row[location_data["subcounty"]]):
            subcounty = str(row[location_data["subcounty"]])
            subcounty_code = _generate_location_abbreviation(
                subcounty, 3, global_used_codes
            )
            code_parts.append(subcounty_code)
        else:
            code_parts.append("UNK")

        # Parish level
        if "parish" in location_data and pd.notna(row[location_data["parish"]]):
            parish = str(row[location_data["parish"]])
            parish_code = _generate_location_abbreviation(parish, 3, global_used_codes)
            code_parts.append(parish_code)
        else:
            code_parts.append("UNK")

        # Village level
        if "village" in location_data and pd.notna(row[location_data["village"]]):
            village = str(row[location_data["village"]])
            village_code = _generate_location_abbreviation(
                village, 3, global_used_codes
            )
            code_parts.append(village_code)
        else:
            code_parts.append("UNK")

        location_codes.append(".".join(code_parts))

    df_coded["location_code"] = location_codes
    return df_coded


def _generate_location_abbreviation(
    name: str, max_length: int = 3, global_used_codes: set = None
) -> str:
    """
    Generate globally unique location abbreviation from name with type awareness.

    Args:
        name: Location name
        max_length: Maximum length of abbreviation
        global_used_codes: Set of ALL used codes globally to avoid duplicates

    Returns:
        Abbreviated location code that is globally unique
    """
    if not name or pd.isna(name):
        return "UNK"

    if global_used_codes is None:
        global_used_codes = set()

    # Clean the name
    clean_name = str(name).strip().upper()

    # Detect location type and generate type-aware abbreviation
    location_type = _detect_location_type(clean_name)

    # Generate base abbreviation
    base_abbrev = _generate_base_abbreviation(clean_name, location_type, max_length)

    # Ensure GLOBAL uniqueness
    final_abbrev = _ensure_unique_code(base_abbrev, global_used_codes, max_length)

    # Add to global used codes
    global_used_codes.add(final_abbrev)

    return final_abbrev


def _detect_location_type(name: str) -> str:
    """Detect the type of location from its name."""
    name_upper = name.upper()

    if "TOWN COUNCIL" in name_upper:
        return "TOWN_COUNCIL"
    elif "SUBCOUNTY" in name_upper or "SUB COUNTY" in name_upper:
        return "SUBCOUNTY"
    elif "WARD" in name_upper:
        return "WARD"
    elif "PARISH" in name_upper:
        return "PARISH"
    elif "VILLAGE" in name_upper or "CELL" in name_upper:
        return "VILLAGE"
    else:
        return "GENERIC"


def _generate_base_abbreviation(name: str, location_type: str, max_length: int) -> str:
    """Generate base abbreviation considering location type."""
    # Remove type-specific words to get the core name
    type_words = [
        "TOWN",
        "COUNCIL",
        "SUBCOUNTY",
        "SUB",
        "COUNTY",
        "WARD",
        "PARISH",
        "VILLAGE",
        "CELL",
    ]
    words = name.split()
    core_words = [w for w in words if w not in type_words]

    if not core_words:
        core_words = [
            w for w in words if w not in ["COUNCIL", "COUNTY"]
        ]  # Keep more essential words

    # Generate core abbreviation
    if len(core_words) == 1:
        core_abbrev = (
            core_words[0][: max_length - 1]
            if max_length > 1
            else core_words[0][:max_length]
        )
    else:
        # Take first letter of each core word
        core_abbrev = "".join([w[0] for w in core_words if w])
        if len(core_abbrev) > max_length - 1 and max_length > 1:
            core_abbrev = core_abbrev[: max_length - 1]

    # Add type suffix if there's room
    if location_type == "TOWN_COUNCIL" and len(core_abbrev) < max_length:
        return core_abbrev + "T"
    elif location_type == "SUBCOUNTY" and len(core_abbrev) < max_length:
        return core_abbrev + "S"
    elif location_type == "WARD" and len(core_abbrev) < max_length:
        return core_abbrev + "W"
    else:
        return core_abbrev[:max_length]


def _ensure_unique_code(base_code: str, used_codes: set, max_length: int) -> str:
    """Ensure the code is unique by adding numeric suffixes if needed."""
    if base_code not in used_codes:
        return base_code

    # Try numeric suffixes
    for i in range(1, 100):
        if max_length <= 2:
            # For very short codes, use single digit
            candidate = base_code[: max_length - 1] + str(i)
        else:
            # For longer codes, use two digits if needed
            suffix = str(i) if i < 10 else str(i)
            candidate = base_code[: max_length - len(suffix)] + suffix

        if candidate not in used_codes:
            return candidate

    # Fallback: use original with random suffix
    import random

    return base_code[: max_length - 2] + str(random.randint(10, 99))


@task(retries=3)
def generate_location_hierarchy_file(
    df: pd.DataFrame,
    output_path: str = "data/processed/locations/location_hierarchy.json",
    config_path: str = "config/location_mappings.yaml",
) -> dict:
    """
    Generate or update location hierarchy JSON file with hierarchical numeric codes.
    Uses format: D##S###P###V#### for District/Subcounty/Parish/Village.
    Merges new locations from facility data with existing hierarchy.

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

    # Load existing hierarchy if it exists
    existing_hierarchy = {}

    try:
        with open(output_path, "r") as f:
            existing_hierarchy = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # No existing hierarchy, start fresh
        existing_hierarchy = {"districts": []}

    # Build hierarchy structure, merging with existing
    hierarchy = existing_hierarchy.copy() if existing_hierarchy else {"districts": []}

    # Build maps for efficient lookup and numbering
    districts_map = {}
    district_counter = len(hierarchy.get("districts", []))

    for district in hierarchy.get("districts", []):
        districts_map[district["id"]] = district

    # Collect all unique locations from the data
    unique_locations = {}
    for _, row in df.iterrows():
        district = (
            str(row[location_data["district"]])
            if "district" in location_data and pd.notna(row[location_data["district"]])
            else None
        )
        subcounty = (
            str(row[location_data["subcounty"]])
            if "subcounty" in location_data
            and pd.notna(row[location_data["subcounty"]])
            else None
        )
        parish = (
            str(row[location_data["parish"]])
            if "parish" in location_data and pd.notna(row[location_data["parish"]])
            else None
        )
        village = (
            str(row[location_data["village"]])
            if "village" in location_data and pd.notna(row[location_data["village"]])
            else None
        )

        if not district:
            continue

        district_slug = slugify(district, separator="-")
        district_id = f"d-{district_slug}"

        if district_id not in unique_locations:
            unique_locations[district_id] = {
                "name": district,
                "district_slug": district_slug,
                "subcounties": {},
            }

        if subcounty:
            # Apply deduplication logic - prefer base names over qualified names
            subcounty_deduplicated = deduplicate_location_name(subcounty, "subcounty")
            subcounty_slug = slugify(subcounty_deduplicated, separator="-")
            subcounty_id = f"s-{district_slug}-{subcounty_slug}"
            if subcounty_id not in unique_locations[district_id]["subcounties"]:
                unique_locations[district_id]["subcounties"][subcounty_id] = {
                    "name": subcounty_deduplicated,
                    "subcounty_slug": subcounty_slug,
                    "parishes": {},
                }

            if parish:
                # Apply deduplication logic - prefer base names over qualified names
                parish_deduplicated = deduplicate_location_name(parish, "parish")
                parish_slug = slugify(parish_deduplicated, separator="-")
                parish_id = f"p-{district_slug}-{subcounty_slug}-{parish_slug}"
                if (
                    parish_id
                    not in unique_locations[district_id]["subcounties"][subcounty_id][
                        "parishes"
                    ]
                ):
                    unique_locations[district_id]["subcounties"][subcounty_id][
                        "parishes"
                    ][parish_id] = {
                        "name": parish_deduplicated,
                        "parish_slug": parish_slug,
                        "villages": {},
                    }

                if village:
                    village_slug = slugify(village, separator="-")
                    village_id = f"v-{district_slug}-{subcounty_slug}-{parish_slug}-{village_slug}"
                    unique_locations[district_id]["subcounties"][subcounty_id][
                        "parishes"
                    ][parish_id]["villages"][village_id] = {
                        "name": village,
                        "village_slug": village_slug,
                    }

    # Process each district
    for district_id, district_data in unique_locations.items():
        if district_id not in districts_map:
            # Create new district
            district_counter += 1
            district_code = f"D{district_counter:02d}"

            new_district = {
                "id": district_id,
                "name": district_data["name"],
                "code": district_code,
                "subcounties": [],
            }
            districts_map[district_id] = new_district
            hierarchy["districts"].append(new_district)

        district_obj = districts_map[district_id]
        district_code = district_obj["code"]

        # Build subcounty map
        subcounties_map = {}
        subcounty_counter = len(district_obj.get("subcounties", []))

        for subcounty in district_obj.get("subcounties", []):
            subcounties_map[subcounty["id"]] = subcounty

        # Process subcounties
        for subcounty_id, subcounty_data in district_data["subcounties"].items():
            if subcounty_id not in subcounties_map:
                # Create new subcounty
                subcounty_counter += 1
                subcounty_code = f"{district_code}S{subcounty_counter:02d}"

                new_subcounty = {
                    "id": subcounty_id,
                    "name": subcounty_data["name"],
                    "code": subcounty_code,
                    "parishes": [],
                }
                subcounties_map[subcounty_id] = new_subcounty
                district_obj["subcounties"].append(new_subcounty)

            subcounty_obj = subcounties_map[subcounty_id]
            subcounty_code = subcounty_obj["code"]

            # Build parish map
            parishes_map = {}
            parish_counter = len(subcounty_obj.get("parishes", []))

            for parish in subcounty_obj.get("parishes", []):
                parishes_map[parish["id"]] = parish

            # Process parishes
            for parish_id, parish_data in subcounty_data["parishes"].items():
                if parish_id not in parishes_map:
                    # Create new parish
                    parish_counter += 1
                    parish_code = f"{subcounty_code}P{parish_counter:02d}"

                    new_parish = {
                        "id": parish_id,
                        "name": parish_data["name"],
                        "code": parish_code,
                        "villages": [],
                    }
                    parishes_map[parish_id] = new_parish
                    subcounty_obj["parishes"].append(new_parish)

                parish_obj = parishes_map[parish_id]
                parish_code = parish_obj["code"]

                # Build village map
                villages_map = {}
                village_counter = len(parish_obj.get("villages", []))

                for village in parish_obj.get("villages", []):
                    villages_map[village["id"]] = village

                # Process villages
                for village_id, village_data in parish_data["villages"].items():
                    if village_id not in villages_map:
                        # Create new village
                        village_counter += 1
                        village_code = f"{parish_code}V{village_counter:02d}"

                        new_village = {
                            "id": village_id,
                            "name": village_data["name"],
                            "code": village_code,
                        }
                        villages_map[village_id] = new_village
                        parish_obj["villages"].append(new_village)

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
    Generate facility IDs for facility data: {facility-name-slug}_{district}

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

    # Find district column
    district_col = None
    for col in df.columns:
        if "district" in col.lower():
            district_col = col
            break

    if facility_name_col and district_col:
        facility_ids = []
        for _, row in df.iterrows():
            facility_name = (
                str(row[facility_name_col])
                if pd.notna(row[facility_name_col])
                else "unknown"
            )
            facility_slug = slugify(facility_name, separator="-")

            district_name = (
                str(row[district_col]) if pd.notna(row[district_col]) else "unknown"
            )
            district_slug = slugify(district_name, separator="-")

            facility_id = f"{facility_slug}_{district_slug}"
            facility_ids.append(facility_id)

        df_with_ids["facility_id"] = facility_ids

    return df_with_ids


@task(retries=3)
def add_thematic_area_column(df: pd.DataFrame, thematic_area: str) -> pd.DataFrame:
    """
    Add thematic_area column to the DataFrame.

    Args:
        df: Input DataFrame
        thematic_area: The thematic area value to add

    Returns:
        DataFrame with thematic_area column added
    """
    df_with_thematic = df.copy()
    df_with_thematic["thematic_area"] = thematic_area
    return df_with_thematic
