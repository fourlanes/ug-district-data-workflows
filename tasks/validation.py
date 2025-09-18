from typing import Dict

import numpy as np
import pandas as pd
import yaml
from prefect import task


@task(retries=3)
def validate_coordinates(
    df: pd.DataFrame, config_path: str = "config/location_mappings.yaml"
) -> Dict:
    """
    Validate coordinates for facility data - check lat/lng within reasonable bounds.

    Args:
        df: Input DataFrame with coordinate data
        config_path: Path to configuration file

    Returns:
        Validation report dictionary
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    validation_report = {
        "total_records": len(df),
        "records_with_coordinates": 0,
        "valid_coordinates": 0,
        "invalid_coordinates": [],
        "missing_coordinates": 0,
        "coordinate_issues": [],
    }

    # Find coordinate columns
    coord_cols = config["coordinate_columns"]
    lat_col = None
    lng_col = None

    for col in df.columns:
        for lat_name in coord_cols["latitude"]:
            if lat_name.lower() in col.lower():
                lat_col = col
                break
        for lng_name in coord_cols["longitude"]:
            if lng_name.lower() in col.lower():
                lng_col = col
                break

    if not lat_col or not lng_col:
        validation_report["coordinate_issues"].append("Coordinate columns not found")
        return validation_report

    # Uganda approximate bounds
    uganda_bounds = {"lat_min": -1.5, "lat_max": 4.0, "lng_min": 29.5, "lng_max": 35.0}

    for idx, row in df.iterrows():
        lat = row[lat_col]
        lng = row[lng_col]

        if pd.isna(lat) or pd.isna(lng):
            validation_report["missing_coordinates"] += 1
            continue

        validation_report["records_with_coordinates"] += 1

        try:
            lat_float = float(lat)
            lng_float = float(lng)

            # Check if coordinates are within Uganda bounds
            if (
                uganda_bounds["lat_min"] <= lat_float <= uganda_bounds["lat_max"]
                and uganda_bounds["lng_min"] <= lng_float <= uganda_bounds["lng_max"]
            ):
                validation_report["valid_coordinates"] += 1
            else:
                validation_report["invalid_coordinates"].append(
                    {
                        "index": idx,
                        "latitude": lat_float,
                        "longitude": lng_float,
                        "issue": "Outside Uganda bounds",
                    }
                )

        except (ValueError, TypeError):
            validation_report["invalid_coordinates"].append(
                {
                    "index": idx,
                    "latitude": lat,
                    "longitude": lng,
                    "issue": "Invalid coordinate format",
                }
            )

    return validation_report


@task(retries=3)
def detect_duplicate_facilities(
    df: pd.DataFrame, config_path: str = "config/location_mappings.yaml"
) -> Dict:
    """
    Detect exact and similar facility matches.

    Args:
        df: Input DataFrame with facility data
        config_path: Path to configuration file

    Returns:
        Duplicate detection report
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    report = {
        "exact_duplicates": [],
        "similar_matches": [],
        "total_duplicates_found": 0,
    }

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
        return report

    # Check for exact duplicates based on name and location
    location_cols = []
    for col in df.columns:
        if any(
            loc_word in col.lower()
            for loc_word in ["district", "subcounty", "parish", "village"]
        ):
            location_cols.append(col)

    if location_cols:
        check_cols = [facility_name_col] + location_cols
        duplicates = df[df.duplicated(subset=check_cols, keep=False)]

        if not duplicates.empty:
            duplicate_groups = duplicates.groupby(check_cols)
            for name_location, group in duplicate_groups:
                if len(group) > 1:
                    report["exact_duplicates"].append(
                        {
                            "facility_name": name_location[0],
                            "location": name_location[1:],
                            "indices": group.index.tolist(),
                        }
                    )

    # Check for similar names (simple similarity check)
    facility_names = df[facility_name_col].dropna().str.lower().str.strip()
    unique_names = facility_names.unique()

    for i, name1 in enumerate(unique_names):
        for j, name2 in enumerate(unique_names[i + 1 :], i + 1):
            # Simple similarity check - same words in different order or minor variations
            words1 = set(name1.split())
            words2 = set(name2.split())
            intersection = words1.intersection(words2)

            # If 80% or more words match, consider it similar
            if (
                len(intersection) >= 0.8 * min(len(words1), len(words2))
                and len(intersection) > 1
            ):
                indices1 = df[
                    df[facility_name_col].str.lower().str.strip() == name1
                ].index.tolist()
                indices2 = df[
                    df[facility_name_col].str.lower().str.strip() == name2
                ].index.tolist()

                report["similar_matches"].append(
                    {
                        "name1": name1,
                        "name2": name2,
                        "indices1": indices1,
                        "indices2": indices2,
                        "similarity_score": len(intersection)
                        / max(len(words1), len(words2)),
                    }
                )

    report["total_duplicates_found"] = len(report["exact_duplicates"]) + len(
        report["similar_matches"]
    )
    return report


@task(retries=3)
def validate_aggregation_totals(df: pd.DataFrame) -> Dict:
    """
    Validate sum consistency for aggregated data.

    Args:
        df: Input DataFrame with aggregated data

    Returns:
        Validation report dictionary
    """
    report = {
        "total_validation_checks": 0,
        "passed_checks": 0,
        "failed_checks": [],
        "warnings": [],
    }

    # Look for total/sum columns and their component columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns

    for col in numeric_cols:
        col_lower = col.lower()
        if "total" in col_lower or "sum" in col_lower:
            # Try to find component columns
            base_name = col_lower.replace("total", "").replace("sum", "").strip("_")

            # Look for columns that might sum to this total
            component_cols = []
            for other_col in numeric_cols:
                if other_col != col and base_name in other_col.lower():
                    component_cols.append(other_col)

            if component_cols:
                report["total_validation_checks"] += 1

                # Check if components sum to total (within tolerance)
                for idx, row in df.iterrows():
                    total_value = row[col]
                    component_sum = row[component_cols].sum()

                    if pd.notna(total_value) and pd.notna(component_sum):
                        tolerance = max(0.01, abs(total_value) * 0.01)  # 1% tolerance

                        if abs(total_value - component_sum) <= tolerance:
                            report["passed_checks"] += 1
                        else:
                            report["failed_checks"].append(
                                {
                                    "index": idx,
                                    "total_column": col,
                                    "total_value": total_value,
                                    "component_sum": component_sum,
                                    "difference": total_value - component_sum,
                                }
                            )

    return report


@task(retries=3)
def validate_percentage_bounds(df: pd.DataFrame) -> Dict:
    """
    Ensure percentages are within 0-100% bounds.

    Args:
        df: Input DataFrame

    Returns:
        Validation report dictionary
    """
    report = {
        "percentage_columns_found": [],
        "out_of_bounds_values": [],
        "total_percentage_values": 0,
        "valid_percentage_values": 0,
    }

    # Find percentage columns
    percentage_cols = []
    for col in df.columns:
        col_lower = col.lower()
        if "%" in col or "percent" in col_lower or "rate" in col_lower:
            percentage_cols.append(col)

    # Also check numeric columns for values that might be percentages
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        sample_values = df[col].dropna().head(10)
        if len(sample_values) > 0:
            # If most values are between 0 and 100, likely a percentage
            in_range = sum(1 for val in sample_values if 0 <= val <= 100)
            if in_range / len(sample_values) >= 0.8:
                percentage_cols.append(col)

    report["percentage_columns_found"] = percentage_cols

    # Validate percentage bounds
    for col in percentage_cols:
        for idx, value in df[col].items():
            if pd.notna(value):
                report["total_percentage_values"] += 1

                try:
                    float_val = float(value)
                    if 0 <= float_val <= 100:
                        report["valid_percentage_values"] += 1
                    else:
                        report["out_of_bounds_values"].append(
                            {
                                "index": idx,
                                "column": col,
                                "value": float_val,
                                "issue": "Outside 0-100% range",
                            }
                        )
                except (ValueError, TypeError):
                    report["out_of_bounds_values"].append(
                        {
                            "index": idx,
                            "column": col,
                            "value": value,
                            "issue": "Invalid percentage format",
                        }
                    )

    return report


@task(retries=3)
def generate_data_quality_report(
    data_type: str, thematic_area: str, validation_results: Dict, file_path: str
) -> Dict:
    """
    Generate comprehensive data quality report.

    Args:
        data_type: Type of data (facility/aggregated)
        thematic_area: Thematic area (health/education/etc)
        validation_results: Combined validation results
        file_path: Original file path

    Returns:
        Comprehensive quality report
    """
    from datetime import datetime

    report = {
        "metadata": {
            "file_path": file_path,
            "data_type": data_type,
            "thematic_area": thematic_area,
            "processing_timestamp": datetime.now().isoformat(),
            "total_records": validation_results.get("total_records", 0),
        },
        "validation_summary": {
            "overall_quality_score": 0,
            "issues_found": [],
            "recommendations": [],
        },
        "detailed_results": validation_results,
    }

    # Calculate overall quality score
    score_components = []

    # Location validation score
    if "location_validation" in validation_results:
        loc_val = validation_results["location_validation"]
        total_loc = loc_val.get("valid_records", 0) + loc_val.get("invalid_records", 0)
        if total_loc > 0:
            loc_score = loc_val.get("valid_records", 0) / total_loc * 100
            score_components.append(loc_score)

    # Coordinate validation score (for facility data)
    if data_type == "facility" and "coordinate_validation" in validation_results:
        coord_val = validation_results["coordinate_validation"]
        total_coord = coord_val.get("records_with_coordinates", 0)
        if total_coord > 0:
            coord_score = coord_val.get("valid_coordinates", 0) / total_coord * 100
            score_components.append(coord_score)

    # Percentage validation score (for aggregated data)
    if data_type == "aggregated" and "percentage_validation" in validation_results:
        pct_val = validation_results["percentage_validation"]
        total_pct = pct_val.get("total_percentage_values", 0)
        if total_pct > 0:
            pct_score = pct_val.get("valid_percentage_values", 0) / total_pct * 100
            score_components.append(pct_score)

    # Calculate overall score
    if score_components:
        report["validation_summary"]["overall_quality_score"] = sum(
            score_components
        ) / len(score_components)
    else:
        report["validation_summary"]["overall_quality_score"] = 0

    # Generate recommendations based on issues found
    recommendations = []
    if report["validation_summary"]["overall_quality_score"] < 80:
        recommendations.append("Data quality is below recommended threshold (80%)")

    if data_type == "facility":
        coord_val = validation_results.get("coordinate_validation", {})
        if coord_val.get("missing_coordinates", 0) > 0:
            recommendations.append("Some facilities are missing coordinate data")
        if coord_val.get("invalid_coordinates"):
            recommendations.append("Some facilities have invalid coordinates")

    report["validation_summary"]["recommendations"] = recommendations

    return report
