import json
from pathlib import Path
from typing import Dict, List

from prefect import task
from slugify import slugify


@task(retries=3)
def clean_location_hierarchy_duplicates(
    hierarchy_path: str = "data/processed/locations/location_hierarchy.json",
    backup_path: str = "data/processed/locations/location_hierarchy_backup.json",
) -> Dict:
    """
    Clean duplicate locations from the hierarchy file while preserving hierarchical codes.

    This function:
    1. Creates a backup of the existing hierarchy
    2. Removes duplicate locations with the same name but different IDs
    3. Regenerates all IDs using consistent format (d-*, s-*, p-*, v-*)
    4. Preserves all existing hierarchical codes (D01S01P01V01 format)
    5. Keeps the most complete entry when merging duplicates
    6. Maintains the overall structure

    Args:
        hierarchy_path: Path to the hierarchy file to clean
        backup_path: Path to save backup before cleaning

    Returns:
        Cleaned hierarchy dictionary
    """
    # Load existing hierarchy
    with open(hierarchy_path, "r") as f:
        hierarchy = json.load(f)

    # Create backup
    Path(backup_path).parent.mkdir(parents=True, exist_ok=True)
    with open(backup_path, "w") as f:
        json.dump(hierarchy, f, indent=2, ensure_ascii=False)

    print(f"Created backup at: {backup_path}")

    # Clean each district
    for district in hierarchy.get("districts", []):
        district_name = district.get("name", "")
        district["subcounties"] = _clean_subcounties(
            district.get("subcounties", []), district_name
        )
        # Regenerate district ID to ensure consistency
        district["id"] = _generate_district_id(district_name)

    # Save cleaned hierarchy
    with open(hierarchy_path, "w") as f:
        json.dump(hierarchy, f, indent=2, ensure_ascii=False)

    print(f"Cleaned hierarchy saved to: {hierarchy_path}")
    return hierarchy


def _clean_subcounties(subcounties: List[Dict], district_name: str = "") -> List[Dict]:
    """Clean duplicate subcounties and regenerate IDs."""
    # Group subcounties by name
    subcounty_groups = {}
    for subcounty in subcounties:
        name = subcounty.get("name", "")
        if name not in subcounty_groups:
            subcounty_groups[name] = []
        subcounty_groups[name].append(subcounty)

    # Merge duplicates within each group
    cleaned_subcounties = []
    for name, group in subcounty_groups.items():
        if len(group) == 1:
            # No duplicates, clean parishes and regenerate ID
            subcounty = group[0]
            subcounty["id"] = _generate_subcounty_id(district_name, name)
            subcounty["parishes"] = _clean_parishes(
                subcounty.get("parishes", []), district_name, name
            )
            cleaned_subcounties.append(subcounty)
        else:
            # Merge duplicates
            print(f"  Merging {len(group)} duplicate subcounties: {name}")
            merged = _merge_subcounty_duplicates(group, district_name)
            cleaned_subcounties.append(merged)

    return cleaned_subcounties


def _merge_subcounty_duplicates(duplicates: List[Dict], district_name: str) -> Dict:
    """Merge duplicate subcounty entries, keeping the most complete one."""
    # Sort by completeness (prefer entries with more parishes, then by ID format)
    duplicates.sort(
        key=lambda x: (
            -len(x.get("parishes", [])),  # More parishes first
            x.get("id", "").startswith("s-"),  # Prefer standard ID format
        )
    )

    # Use the most complete entry as base
    base = duplicates[0].copy()
    subcounty_name = base.get("name", "")

    # Regenerate ID for consistency
    base["id"] = _generate_subcounty_id(district_name, subcounty_name)

    # Collect all parishes from all duplicates
    all_parishes = []
    for duplicate in duplicates:
        all_parishes.extend(duplicate.get("parishes", []))

    # Clean and merge parishes
    base["parishes"] = _clean_parishes(all_parishes, district_name, subcounty_name)

    return base


def _clean_parishes(
    parishes: List[Dict], district_name: str = "", subcounty_name: str = ""
) -> List[Dict]:
    """Clean duplicate parishes and regenerate IDs."""
    # Group parishes by name
    parish_groups = {}
    for parish in parishes:
        name = parish.get("name", "")
        if name not in parish_groups:
            parish_groups[name] = []
        parish_groups[name].append(parish)

    # Merge duplicates within each group
    cleaned_parishes = []
    for name, group in parish_groups.items():
        if len(group) == 1:
            # No duplicates, clean villages and regenerate ID
            parish = group[0]
            parish["id"] = _generate_parish_id(district_name, subcounty_name, name)
            parish["villages"] = _clean_villages(
                parish.get("villages", []), district_name, subcounty_name, name
            )
            cleaned_parishes.append(parish)
        else:
            # Merge duplicates
            print(f"    Merging {len(group)} duplicate parishes: {name}")
            merged = _merge_parish_duplicates(group, district_name, subcounty_name)
            cleaned_parishes.append(merged)

    return cleaned_parishes


def _merge_parish_duplicates(
    duplicates: List[Dict], district_name: str, subcounty_name: str
) -> Dict:
    """Merge duplicate parish entries, keeping the most complete one."""
    # Sort by completeness (prefer entries with more villages, then by ID format)
    duplicates.sort(
        key=lambda x: (
            -len(x.get("villages", [])),  # More villages first
            x.get("id", "").startswith("p-"),  # Prefer standard ID format
        )
    )

    # Use the most complete entry as base
    base = duplicates[0].copy()
    parish_name = base.get("name", "")

    # Regenerate ID for consistency
    base["id"] = _generate_parish_id(district_name, subcounty_name, parish_name)

    # Collect all villages from all duplicates
    all_villages = []
    for duplicate in duplicates:
        all_villages.extend(duplicate.get("villages", []))

    # Clean and merge villages
    base["villages"] = _clean_villages(
        all_villages, district_name, subcounty_name, parish_name
    )

    return base


def _clean_villages(
    villages: List[Dict],
    district_name: str = "",
    subcounty_name: str = "",
    parish_name: str = "",
) -> List[Dict]:
    """Clean duplicate villages and regenerate IDs."""
    # Group villages by name
    village_groups = {}
    for village in villages:
        name = village.get("name", "")
        if name not in village_groups:
            village_groups[name] = []
        village_groups[name].append(village)

    # Merge duplicates within each group
    cleaned_villages = []
    for name, group in village_groups.items():
        if len(group) == 1:
            # No duplicates, regenerate ID
            village = group[0]
            village["id"] = _generate_village_id(
                district_name, subcounty_name, parish_name, name
            )
            cleaned_villages.append(village)
        else:
            # Merge duplicates - prefer standard ID format and regenerate ID
            print(f"      Merging {len(group)} duplicate villages: {name}")
            group.sort(key=lambda x: x.get("id", "").startswith("v-"))
            village = group[0]  # Keep the first (most preferred)
            village["id"] = _generate_village_id(
                district_name, subcounty_name, parish_name, name
            )
            cleaned_villages.append(village)

    return cleaned_villages


@task(retries=3)
def validate_cleaned_hierarchy(
    hierarchy_path: str = "data/processed/locations/location_hierarchy.json",
) -> Dict:
    """
    Validate the cleaned hierarchy for remaining duplicates and structure integrity.

    Args:
        hierarchy_path: Path to the cleaned hierarchy file

    Returns:
        Validation report dictionary
    """
    with open(hierarchy_path, "r") as f:
        hierarchy = json.load(f)

    report = {
        "total_districts": 0,
        "total_subcounties": 0,
        "total_parishes": 0,
        "total_villages": 0,
        "duplicate_subcounties": [],
        "duplicate_parishes": [],
        "duplicate_villages": [],
        "validation_passed": True,
    }

    for district in hierarchy.get("districts", []):
        report["total_districts"] += 1

        # Check subcounty duplicates
        subcounty_names = [s.get("name", "") for s in district.get("subcounties", [])]
        subcounty_duplicates = [
            name for name in set(subcounty_names) if subcounty_names.count(name) > 1
        ]
        if subcounty_duplicates:
            report["duplicate_subcounties"].extend(
                [(district.get("name", ""), dup) for dup in subcounty_duplicates]
            )
            report["validation_passed"] = False

        for subcounty in district.get("subcounties", []):
            report["total_subcounties"] += 1

            # Check parish duplicates
            parish_names = [p.get("name", "") for p in subcounty.get("parishes", [])]
            parish_duplicates = [
                name for name in set(parish_names) if parish_names.count(name) > 1
            ]
            if parish_duplicates:
                report["duplicate_parishes"].extend(
                    [(subcounty.get("name", ""), dup) for dup in parish_duplicates]
                )
                report["validation_passed"] = False

            for parish in subcounty.get("parishes", []):
                report["total_parishes"] += 1

                # Check village duplicates
                village_names = [v.get("name", "") for v in parish.get("villages", [])]
                village_duplicates = [
                    name for name in set(village_names) if village_names.count(name) > 1
                ]
                if village_duplicates:
                    report["duplicate_villages"].extend(
                        [(parish.get("name", ""), dup) for dup in village_duplicates]
                    )
                    report["validation_passed"] = False

                report["total_villages"] += len(parish.get("villages", []))

    print(f"Validation Report:")
    print(
        f"  Total locations: {report['total_districts']} districts, {report['total_subcounties']} subcounties, {report['total_parishes']} parishes, {report['total_villages']} villages"
    )

    if report["validation_passed"]:
        print("  ✅ No duplicates found - hierarchy is clean!")
    else:
        if report["duplicate_subcounties"]:
            print(
                f"  ❌ Found {len(report['duplicate_subcounties'])} duplicate subcounties"
            )
        if report["duplicate_parishes"]:
            print(f"  ❌ Found {len(report['duplicate_parishes'])} duplicate parishes")
        if report["duplicate_villages"]:
            print(f"  ❌ Found {len(report['duplicate_villages'])} duplicate villages")

    return report


def _generate_district_id(name: str) -> str:
    """Generate consistent district ID."""
    return f"d-{slugify(name, separator='-')}"


def _generate_subcounty_id(district_name: str, subcounty_name: str) -> str:
    """Generate consistent subcounty ID."""
    district_slug = slugify(district_name, separator="-")
    subcounty_slug = slugify(subcounty_name, separator="-")
    return f"s-{district_slug}-{subcounty_slug}"


def _generate_parish_id(
    district_name: str, subcounty_name: str, parish_name: str
) -> str:
    """Generate consistent parish ID."""
    district_slug = slugify(district_name, separator="-")
    subcounty_slug = slugify(subcounty_name, separator="-")
    parish_slug = slugify(parish_name, separator="-")
    return f"p-{district_slug}-{subcounty_slug}-{parish_slug}"


def _generate_village_id(
    district_name: str, subcounty_name: str, parish_name: str, village_name: str
) -> str:
    """Generate consistent village ID."""
    district_slug = slugify(district_name, separator="-")
    subcounty_slug = slugify(subcounty_name, separator="-")
    parish_slug = slugify(parish_name, separator="-")
    village_slug = slugify(village_name, separator="-")
    return f"v-{district_slug}-{subcounty_slug}-{parish_slug}-{village_slug}"
