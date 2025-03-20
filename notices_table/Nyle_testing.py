import os
import requests
import json
from datetime import date, datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple
from supabase import create_client, Client
from dotenv import load_dotenv

def highergov_get_all_awards(
  api_key                : str,
  award_id               : Optional[str] = None,
  awardee_key            : Optional[int] = None,
  awardee_key_parent     : Optional[int] = None,
  awardee_uei            : Optional[str] = None,
  awardee_uei_parent     : Optional[str] = None,
  awarding_agency_key    : Optional[int] = None,
  funding_agency_key     : Optional[int] = None,
  last_modified_date     : Optional[Union[str, date]] = None,
  naics_code             : Optional[str] = None,
  ordering               : Optional[str] = None,
  page_size              : int = 10,
  parent_award_id        : Optional[str] = None,
  psc_code               : Optional[str] = None,
  search_id              : Optional[str] = None,
  vehicle_key            : Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Fetch all awards from the HigherGov API by iterating through all pages.

    Args:
        api_key: API key for authentication
        award_id: The government Award ID (e.g., "70RDAD21D00000002-70CDCR22FR0000013")
        awardee_key: HigherGov Awardee Key
        awardee_key_parent: HigherGov Awardee Key (Parent Level)
        awardee_uei: Awardee UEI (e.g., "SMNWM6HN79X5")
        awardee_uei_parent: Awardee UEI Parent
        awarding_agency_key: HigherGov Awarding Agency key
        funding_agency_key: HigherGov Funding Agency key
        last_modified_date: Last modified date filter (format: YYYY-MM-DD)
        naics_code: Awards NAICS code (e.g., "541330")
        ordering: Field to use when ordering the results (e.g., "-last_modified_date")
        page_size: Number of records returned per page (max 100)
        parent_award_id: The government Award ID of the parent Award
        psc_code: PSC code (e.g., "8440")
        search_id: HigherGov SearchID
        vehicle_key: HigherGov Vehicle key

    Returns:
        List of contract dictionaries retrieved from the API

    Raises:
        ValueError: If api_key is missing or page_size exceeds 100
        requests.exceptions.RequestException: For unrecoverable network errors
    """
    if not api_key:
        raise ValueError("API key is required")

    if page_size > 100:
        raise ValueError("page_size cannot exceed 100")

    # Convert date object to string if needed
    if isinstance(last_modified_date, date):
        last_modified_date = last_modified_date.isoformat()

    url = "https://www.highergov.com/api-external/contract/"
    all_contracts: List[Dict[str, Any]] = []
    page_number = 1

    while True:
        # Build request parameters
        params = {
            'api_key': api_key,
            'award_id': award_id,
            'awardee_key': awardee_key,
            'awardee_key_parent': awardee_key_parent,
            'awardee_uei': awardee_uei,
            'awardee_uei_parent': awardee_uei_parent,
            'awarding_agency_key': awarding_agency_key,
            'funding_agency_key': funding_agency_key,
            'last_modified_date': last_modified_date,
            'naics_code': naics_code,
            'ordering': ordering,
            'page_number': page_number,
            'page_size': page_size,
            'parent_award_id': parent_award_id,
            'psc_code': psc_code,
            'search_id': search_id,
            'vehicle_key': vehicle_key
        }
        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}

        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()
        contracts = data.get('results', [])
        all_contracts.extend(contracts)

        # Check if we've reached the last page
        if len(contracts) < page_size:
            break

        # Move to next page
        page_number += 1

    return all_contracts

def highergov_get_all_opportunities(
    api_key: str,
    agency_key: Optional[int] = None,
    captured_date: Optional[Union[str, date]] = None,
    opp_key: Optional[str] = None,
    ordering: Optional[str] = None,
    page_size: int = 10,
    posted_date: Optional[Union[str, date]] = None,
    search_id: Optional[str] = None,
    source_id: Optional[str] = None,
    source_type: Optional[str] = None,
    version_key: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetch all opportunities from the HigherGov API by iterating through all pages.

    Args:
        api_key: API key for authentication
        agency_key: HigherGov Agency key
        captured_date: Date the opportunity was added to HigherGov (format: YYYY-MM-DD)
        opp_key: The HigherGov opportunity key
        ordering: Field to use when ordering the results
                 (e.g., -captured_date, -due_date, -posted_date)
        page_size: Number of records returned per page (max 100)
        posted_date: Date the opportunity was posted by the agency (format: YYYY-MM-DD)
        search_id: HigherGov SearchID
        source_id: The source opportunity ID
        source_type: Opportunity source type (sam, dibbs, sbir, grant, sled)
        version_key: The HigherGov opportunity version key

    Returns:
        List of opportunity dictionaries retrieved from the API

    Raises:
        ValueError: If api_key is missing or page_size exceeds 100
        requests.exceptions.RequestException: For unrecoverable network errors
    """
    if not api_key:
        raise ValueError("API key is required")

    if page_size > 100:
        raise ValueError("page_size cannot exceed 100")

    # Convert date objects to strings if needed
    if isinstance(captured_date, date):
        captured_date = captured_date.isoformat()
    if isinstance(posted_date, date):
        posted_date = posted_date.isoformat()

    url = "https://www.highergov.com/api-external/opportunity/"
    all_opportunities: List[Dict[str, Any]] = []
    page_number = 1

    while True:
        # Build request parameters
        params = {
            'api_key': api_key,
            'agency_key': agency_key,
            'captured_date': captured_date,
            'opp_key': opp_key,
            'ordering': ordering,
            'page_number': page_number,
            'page_size': page_size,
            'posted_date': posted_date,
            'search_id': search_id,
            'source_id': source_id,
            'source_type': source_type,
            'version_key': version_key
        }
        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}

        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()
        opportunities = data.get('results', [])
        all_opportunities.extend(opportunities)

        # Check if we've reached the last page
        if len(opportunities) < page_size:
            break

        # Move to next page
        page_number += 1

    return all_opportunities

def get_award_by_piid(supabase: Client, piid: str) -> Dict[str, Any]:
    """
    Retrieve an award from Supabase's Awards table by its piid.

    Args:
        supabase (Client): Initialized Supabase client
        piid (str): The piid (Procurement Instrument Identifier) of the award

    Returns:
        Dict[str, Any]: Award data if found, empty dict if not found

    Raises:
        ValueError: If piid is empty or invalid
        Exception: For other database errors
    """
    # Input validation
    if not piid or not isinstance(piid, str):
        raise ValueError("piid must be a non-empty string")

    try:
        # Query the Awards table
        response = supabase.table("awards") \
            .select("*") \
            .eq("piid", piid) \
            .limit(1) \
            .execute()

        # Check if we got any data back
        if response.data and len(response.data) > 0:
            return response.data[0]
        return {}

    except Exception as e:
        print(f"Error fetching award with piid {piid}: {str(e)}")
        raise Exception(f"Database error: {str(e)}")

def get_nested_value(obj: Dict[str, Any], field: str) -> Any:
    """Helper function to get nested dictionary values using dot notation."""
    try:
        for key in field.split('.'):
            obj = obj[key]
        return obj
    except (KeyError, TypeError, AttributeError):
        return None

def standardize_value(value: Any) -> Any:
    """Helper function to standardize values for comparison."""
    if value is None:
        return None

    # Convert to string and strip whitespace
    if isinstance(value, str):
        return value.strip()

    # Convert numeric types to float for consistent comparison
    if isinstance(value, (int, float)):
        return float(value)

    return value

def safe_compare_text(x: Optional[str], y: Optional[str], condition: callable) -> bool:
    """Helper function to safely compare text values that might be None."""
    if x is None or y is None:
        return False
    return condition(x, y)

def compare_award_data(
    api_key: str,
    supabase: Client,
    award_id: str,
    piid: str
) -> Dict[str, Any]:
    """
    Compare award data between HigherGov API and Supabase database.

    Args:
        api_key (str): HigherGov API key
        supabase (Client): Initialized Supabase client
        award_id (str): Full award ID for HigherGov API
        piid (str): PIID for Supabase query

    Returns:
        Dict containing comparison results with any mismatches found

    Raises:
        ValueError: If required parameters are missing
        Exception: For API or database errors
    """
    # Fetch data from both sources
    higher_gov_data = highergov_get_all_awards(api_key=api_key, award_id=award_id)
    supabase_data = get_award_by_piid(supabase=supabase, piid=piid)

    if not higher_gov_data or not supabase_data:
        raise ValueError("No data found in one or both sources")

    # Extract HigherGov data (first result since it returns a list)
    hg = higher_gov_data[0]
    sb = supabase_data

    print("HigherGov Data:")
    print(json.dumps(hg, indent=4))
    print("Supabase Data:")
    print(json.dumps(sb, indent=4))

    # Define fields to compare with their mappings
    comparison_results = {
        "matches": [],
        "mismatches": [],
        "higher_gov_only": [],
        "supabase_only": [],
        "unmapped_higher_gov_fields": [],
        "unmapped_supabase_fields": []
    }

    # Updated field mappings to include previously unmapped fields
    field_mappings = [
        # Basic Award Information
        ("award_id", "piid", "Award ID/PIID"),
        ("parent_award_id", "parent_award_piid", "Parent Award ID"),
        ("award_description_original", "description", "Description"),
        ("award_type", "type_description", "Award Type"),
        ("solicitation_identifier", "solicitation_identifier", "Solicitation ID"),

        # Financial Information
        ("total_dollars_obligated", "total_obligation", "Total Obligation"),
        ("current_total_value_of_award", "base_exercised_options", "Current Total Value"),
        ("potential_total_value_of_award", "base_and_all_options", "Potential Total Value"),

        # Dates
        ("period_of_performance_start_date", "period_of_performance_start_date", "Performance Start Date"),
        ("period_of_performance_current_end_date", "period_of_performance_end_date", "Performance End Date"),
        ("period_of_performance_potential_end_date", "period_of_performance_potential_end_date", "Potential End Date"),
        ("last_modified_date", "period_of_performance_last_modified_date", "Last Modified Date"),

        # Recipient Information
        ("awardee.clean_name", "recipient_name", "Recipient Name"),
        ("awardee.uei", "recipient_uei", "Recipient UEI"),
        ("awardee_parent.clean_name", "parent_recipient_name", "Parent Recipient Name"),
        ("awardee_parent.uei", "parent_recipient_uei", "Parent Recipient UEI"),

        # Place of Performance
        ("primary_place_of_performance_city_name", "place_of_performance_city_name", "Place of Performance City"),
        ("primary_place_of_performance_state_code", "place_of_performance_state_code", "Place of Performance State"),
        ("primary_place_of_performance_country_name", "place_of_performance_country_name", "Place of Performance Country"),
        ("primary_place_of_performance_zip", "place_of_performance_zip5", "Place of Performance ZIP"),

        # Classification Codes
        ("psc_code.psc_code", "product_or_service_code", "PSC Code"),
        ("naics_code.naics_code", "naics", "NAICS Code"),
        ("naics_code.naics_description", "naics_description", "NAICS Description"),

        # Competition Information
        ("number_of_offers_received", "number_of_offers_received", "Number of Offers"),
        ("extent_competed", "extent_competed_description", "Extent Competed"),
        ("solicitation_procedures", "solicitation_procedures_description", "Solicitation Procedures"),
        ("type_of_contract_pricing_description", "type_of_contract_pricing_description", "Contract Pricing Type"),

        # Additional Details
        ("subcontracting_plan", "subcontracting_plan_description", "Subcontracting Plan"),
        ("clinger_cohen_act_planning", "clinger_cohen_act_planning_description", "Clinger Cohen Act Planning"),

        # Agency Information
        ("awarding_agency.agency_name", "awarding_agency_subtier_agency_name", "Awarding Agency"),
        ("funding_agency.agency_name", "funding_agency_subtier_agency_name", "Funding Agency"),

        # Transaction Information
        ("latest_action_date", "transactions.0.action_date", "Latest Action Date"),
        ("latest_transaction_key", "transactions.0.id", "Latest Transaction ID"),

        # Additional Award Details
        ("award_type", "type_description", "Award Type"),
        ("category", "category", "Award Category"),
        ("latest_action_date", "date_signed", "Award Date"),
        ("latest_action_date_fiscal_year", "fiscal_year", "Fiscal Year"),

        # Additional Recipient Details
        ("awardee.cage_code", "recipient_cage_code", "Recipient CAGE Code"),
        ("recipient_location_address_line1", "recipient_location_address_line1", "Recipient Address"),
        ("recipient_location_zip4", "recipient_location_zip4", "Recipient ZIP+4"),
        ("recipient_location_zip5", "recipient_location_zip5", "Recipient ZIP5"),
        ("recipient_location_congressional_code", "recipient_location_congressional_code", "Recipient Congressional District"),
        ("business_categories", "business_categories", "Business Categories"),

        # Additional Place of Performance Details
        ("primary_place_of_performance_county_name", "place_of_performance_county_name", "Place of Performance County"),
        ("primary_place_of_performance_congressional_code", "place_of_performance_congressional_code", "Place of Performance Congressional District"),
        ("primary_place_of_performance_zip4", "place_of_performance_zip4", "Place of Performance ZIP+4"),

        # Extended Classification Details
        ("psc_code.psc_name", "psc_hierarchy_base_code_description", "PSC Description"),
        ("psc_code.psc_description", "product_or_service_description", "PSC Full Description"),
        ("naics_code.naics_description", "naics_hierarchy_base_code_description", "NAICS Description"),

        # Competition and Contract Details
        ("type_of_set_aside", "type_set_aside", "Set Aside Type"),
        ("other_than_full_and_open_competition", "other_than_full_and_open", "Other Than Full and Open Competition"),
        ("commercial_item_acquisition", "commercial_item_acquisition", "Commercial Item Acquisition"),
        ("consolidated_contract", "consolidated_contract", "Consolidated Contract"),
        ("multi_year_contract", "multi_year_contract", "Multi Year Contract"),
        ("purchase_card_as_payment_method", "purchase_card_as_payment_method", "Purchase Card as Payment Method"),

        # Agency Details
        ("funding_agency.agency_name", "funding_agency_office_agency_name", "Funding Office Name"),
        ("awarding_agency.agency_name", "awarding_agency_office_agency_name", "Awarding Office Name"),
        ("funding_agency_toptier_agency_name", "funding_agency_toptier_agency_name", "Funding Agency Top Tier Name"),
        ("awarding_agency_toptier_agency_name", "awarding_agency_toptier_agency_name", "Awarding Agency Top Tier Name"),

        # Financial Details
        ("total_account_obligation", "total_account_obligation", "Total Account Obligation"),
        ("total_account_outlay", "total_account_outlay", "Total Account Outlay"),

        # System/Reference IDs
        ("generated_unique_award_id", "generated_unique_award_id", "Generated Unique Award ID"),
        ("usa_spending_id", "usa_spending_id", "USA Spending ID"),

        # Custom comparisons for fields that need special handling
        ("type_of_contract_pricing_description", "type_of_contract_pricing_description", "Contract Pricing Type",
         lambda x, y: safe_compare_text(x, y, lambda a, b: a == "Fixed Price" and b == "FIRM FIXED PRICE")),

        ("extent_competed", "extent_competed_description", "Extent Competed",
         lambda x, y: safe_compare_text(x, y, lambda a, b: a == "Not Competed" and b == "NOT COMPETED")),

        ("solicitation_procedures", "solicitation_procedures_description", "Solicitation Procedures",
         lambda x, y: safe_compare_text(x, y, lambda a, b: a == "Sole Source" and b == "ONLY ONE SOURCE")),

        ("subcontracting_plan", "subcontracting_plan_description", "Subcontracting Plan",
         lambda x, y: safe_compare_text(x, y, lambda a, b: a == "Plan Not Required" and b == "PLAN NOT REQUIRED")),

        ("clinger_cohen_act_planning", "clinger_cohen_act_planning_description", "Clinger Cohen Act Planning",
         lambda x, y: safe_compare_text(x, y, lambda a, b: a == "No" and b == "NO")),

        ("domestic_or_foreign_entity_description", "domestic_or_foreign_entity_description", "Domestic/Foreign Entity",
         lambda x, y: safe_compare_text(x, y, lambda a, b: "U.S." in a and "U.S." in b))
    ]

    # Compare each field
    for mapping in field_mappings:
        # Unpack mapping values, with custom_compare being optional
        hg_field, sb_field, display_name = mapping[:3]
        custom_compare = mapping[3] if len(mapping) > 3 else None

        hg_value = get_nested_value(hg, hg_field)
        sb_value = get_nested_value(sb, sb_field)

        # Convert to same type for comparison
        hg_value = standardize_value(hg_value)
        sb_value = standardize_value(sb_value)

        # Use custom comparison function if provided, otherwise use direct equality
        if custom_compare:
            matches = custom_compare(hg_value, sb_value)
        else:
            matches = hg_value == sb_value

        if matches:
            comparison_results["matches"].append({
                "field": display_name,
                "value": f"{hg_value} -> {sb_value}" if custom_compare else hg_value
            })
        else:
            comparison_results["mismatches"].append({
                "field": display_name,
                "higher_gov_value": hg_value,
                "supabase_value": sb_value
            })

    # Find unmapped fields in HigherGov data
    for field in hg.keys():
        if field not in [f[0] for f in field_mappings]:
            value = hg[field]
            if isinstance(value, dict):
                # For nested objects, include all nested fields
                for nested_field, nested_value in _flatten_dict(value, parent_key=field).items():
                    comparison_results["unmapped_higher_gov_fields"].append({
                        "field": nested_field,
                        "value": nested_value
                    })
            else:
                comparison_results["unmapped_higher_gov_fields"].append({
                    "field": field,
                    "value": value
                })

    # Find unmapped fields in Supabase data
    for field in sb.keys():
        if field not in [f[1] for f in field_mappings]:
            value = sb[field]
            if isinstance(value, dict):
                # For nested objects, include all nested fields
                for nested_field, nested_value in _flatten_dict(value, parent_key=field).items():
                    comparison_results["unmapped_supabase_fields"].append({
                        "field": nested_field,
                        "value": nested_value
                    })
            else:
                comparison_results["unmapped_supabase_fields"].append({
                    "field": field,
                    "value": value
                })

    return comparison_results

def get_opportunity_by_solicitation_id(supabase: Client, solicitation_id: str) -> Dict[str, Any]:
    """
    Retrieve an opportunity from Supabase by its solicitation_id, including the most recent notice data.

    Args:
        supabase (Client): Initialized Supabase client
        solicitation_id (str): The solicitation ID of the opportunity

    Returns:
        Dict[str, Any]: Latest notice data if found, empty dict if not found

    Raises:
        ValueError: If solicitation_id is empty or invalid
        Exception: For other database errors
    """
    if not solicitation_id or not isinstance(solicitation_id, str):
        raise ValueError("solicitation_id must be a non-empty string")

    try:
        # First get the solicitation data to get the latest_notice_id
        solicitation_response = supabase.table("solicitations") \
            .select("latest_notice_id") \
            .eq("solicitation_id", solicitation_id) \
            .is_("deleted", False) \
            .limit(1) \
            .execute()

        if not solicitation_response.data:
            return {}

        latest_notice_id = solicitation_response.data[0].get("latest_notice_id")
        if not latest_notice_id:
            return {}

        # Get the notice data using the latest_notice_id
        notice_response = supabase.table("notices") \
            .select("*") \
            .eq("notice_id", latest_notice_id) \
            .limit(1) \
            .execute()

        if not notice_response.data:
            return {}

        return notice_response.data[0]

    except Exception as e:
        print(f"Error fetching opportunity with solicitation_id {solicitation_id}: {str(e)}")
        raise Exception(f"Database error: {str(e)}")

def get_naics_id_by_code(supabase: Client, naics_code: str) -> Optional[int]:
    """Get NAICS ID from the NAICS table using the code."""
    try:
        response = supabase.table("naics") \
            .select("naics_id") \
            .eq("naics_code", naics_code) \
            .limit(1) \
            .execute()

        if response.data:
            return response.data[0].get("naics_id")
        return None
    except Exception as e:
        print(f"Error fetching NAICS ID for code {naics_code}: {str(e)}")
        return None

def get_psc_id_by_code(supabase: Client, psc_code: str) -> Optional[int]:
    """Get PSC ID from the PSC table using the code."""
    try:
        response = supabase.table("psc") \
            .select("psc_id") \
            .eq("psc_code", psc_code) \
            .limit(1) \
            .execute()

        if response.data:
            return response.data[0].get("psc_id")
        return None
    except Exception as e:
        print(f"Error fetching PSC ID for code {psc_code}: {str(e)}")
        return None

def compare_opportunity_data(
    api_key: str,
    supabase: Client,
    solicitation_id: str
) -> Dict[str, Any]:
    """
    Compare opportunity data between HigherGov API and Supabase database.

    Args:
        api_key (str): HigherGov API key
        supabase (Client): Initialized Supabase client
        solicitation_id (str): Solicitation ID to compare

    Returns:
        Dict containing comparison results with matches, mismatches, and unmapped fields

    Raises:
        ValueError: If required parameters are missing or no data found
        Exception: For API or database errors
    """
    # Fetch data from both sources
    higher_gov_data = highergov_get_all_opportunities(api_key=api_key, source_id=solicitation_id)
    supabase_data = get_opportunity_by_solicitation_id(supabase=supabase, solicitation_id=solicitation_id)

    if not higher_gov_data or not supabase_data:
        raise ValueError("No data found in one or both sources")

    # Extract HigherGov data (first result since it returns a list)
    hg = higher_gov_data[0]
    sb = supabase_data

    print("HigherGov Data:")
    print(json.dumps(hg, indent=4))
    print("Supabase Data:")
    print(json.dumps(sb, indent=4))

    # Track which fields we've compared
    hg_fields_checked = set()
    sb_fields_checked = set()

    comparison_results = {
        "matches": [],
        "mismatches": [],
        "unmapped_higher_gov_fields": [],
        "unmapped_supabase_fields": []
    }

    # Define type mapping between HigherGov and Supabase
    type_mapping = {
        "Award Notice": "a",
        "Foreign Government Standard": "f",
        "Sale of Surplus Property": "g",
        "Consolidate/(Substantially) Bundle": "i",
        "Justification and Approval (J&A)": "j",
        "Combined Synopsis/Solicitation": "k",
        "Fair Opportunity / Limited Sources Justification": "l",
        "Modification/Amendment": "m",
        "Solicitation": "o",
        "Presolicitation": "p",
        "Sources Sought": "r",
        "Special Notice": "s",
        "Justification": "u"
    }

    # Get NAICS and PSC IDs if codes are available
    naics_code = get_nested_value(hg, "naics_code.naics_code")
    psc_code = get_nested_value(hg, "psc_code.psc_code")

    naics_id = get_naics_id_by_code(supabase, naics_code) if naics_code else None
    psc_id = get_psc_id_by_code(supabase, psc_code) if psc_code else None

    # Updated field mappings based on new schema
    field_mappings = [
        # Basic Opportunity Information
        ("source_id", "solicitation_id", "Solicitation ID"),
        ("source_id_version", "notice_id", "Notice ID"),
        ("title", "title", "Title"),
        ("description_text", "description_body", "Description"),
        ("opp_type.description", "type", "Notice Type", lambda x, y: type_mapping.get(x) == y),

        # Dates
        ("posted_date", "posted_date", "Posted Date"),
        ("due_date", "solicitation_response_deadline", "Response Deadline"),

        # Agency Information
        ("agency.agency_name", "organization_level_1_name", "Department Name"),

        # Classification Codes
        ("naics_code.naics_code", "naics", "NAICS Code"),
        ("psc_code.psc_code", "psc", "PSC Code"),
        # Add custom comparisons for IDs
        ("naics_code.naics_code", "naics_id", "NAICS ID", lambda x, _: get_naics_id_by_code(supabase, x) == _),
        ("psc_code.psc_code", "psc_id", "PSC ID", lambda x, _: get_psc_id_by_code(supabase, x) == _),

        # Set Aside Information
        ("set_aside", "solicitation_set_aside", "Set Aside Type"),

        # Primary Contact Information
        ("primary_contact_email.contact_title", "primary_poc_title", "Primary Contact Title"),
        ("primary_contact_email.contact_name", "primary_poc_full_name", "Primary Contact Name"),
        ("primary_contact_email.contact_email", "primary_poc_email", "Primary Contact Email"),
        ("primary_contact_email.contact_phone", "primary_poc_phone", "Primary Contact Phone"),

        # Secondary Contact Information
        ("secondary_contact_email.contact_title", "secondary_poc_title", "Secondary Contact Title"),
        ("secondary_contact_email.contact_name", "secondary_poc_full_name", "Secondary Contact Name"),
        ("secondary_contact_email.contact_email", "secondary_poc_email", "Secondary Contact Email"),
        ("secondary_contact_email.contact_phone", "secondary_poc_phone", "Secondary Contact Phone"),

        # Location Information
        ("pop_city", "pop_city_name", "City"),
        ("pop_state", "pop_state_name", "State"),
        ("pop_zip", "pop_zip", "ZIP Code"),
        ("pop_country", "pop_country_name", "Country"),

        # Award Information (if available)
        ("award_amount", "award_amount", "Award Amount"),
        ("award_date", "award_date", "Award Date"),
        ("awardee_name", "awardee_name", "Awardee Name"),
        ("awardee_uei", "awardee_uei", "Awardee UEI")
    ]

    # Compare mapped fields
    for mapping in field_mappings:
        hg_field, sb_field, display_name = mapping[:3]
        custom_compare = mapping[3] if len(mapping) > 3 else None

        # Track which fields we've checked
        hg_fields_checked.add(hg_field.split('.')[0])
        sb_fields_checked.add(sb_field.split('.')[0])

        hg_value = get_nested_value(hg, hg_field)
        sb_value = get_nested_value(sb, sb_field)

        # Convert to same type for comparison
        hg_value = standardize_value(hg_value)
        sb_value = standardize_value(sb_value)

        # Use custom comparison function if provided, otherwise use direct equality
        if custom_compare:
            matches = custom_compare(hg_value, sb_value)
        else:
            matches = hg_value == sb_value

        if matches:
            comparison_results["matches"].append({
                "field": display_name,
                "value": f"{hg_value} -> {sb_value}" if custom_compare else hg_value
            })
        else:
            comparison_results["mismatches"].append({
                "field": display_name,
                "higher_gov_value": hg_value,
                "supabase_value": sb_value
            })

    # Find unmapped fields in HigherGov data
    for field in hg.keys():
        if field not in hg_fields_checked:
            value = hg[field]
            if isinstance(value, dict):
                # For nested objects, include all nested fields
                for nested_field, nested_value in _flatten_dict(value, parent_key=field).items():
                    comparison_results["unmapped_higher_gov_fields"].append({
                        "field": nested_field,
                        "value": nested_value
                    })
            else:
                comparison_results["unmapped_higher_gov_fields"].append({
                    "field": field,
                    "value": value
                })

    # Find unmapped fields in Supabase data
    for field in sb.keys():
        if field not in sb_fields_checked:
            value = sb[field]
            if isinstance(value, dict):
                # For nested objects, include all nested fields
                for nested_field, nested_value in _flatten_dict(value, parent_key=field).items():
                    comparison_results["unmapped_supabase_fields"].append({
                        "field": nested_field,
                        "value": nested_value
                    })
            else:
                comparison_results["unmapped_supabase_fields"].append({
                    "field": field,
                    "value": value
                })

    return comparison_results

def _flatten_dict(d: Dict[str, Any], parent_key: str = '') -> Dict[str, Any]:
    """
    Helper function to flatten nested dictionaries with dot notation.

    Args:
        d: Dictionary to flatten
        parent_key: Parent key for nested dictionaries

    Returns:
        Flattened dictionary with dot notation keys
    """
    items: List[Tuple[str, Any]] = []
    for k, v in d.items():
        new_key = f"{parent_key}.{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(_flatten_dict(v, new_key).items())
        else:
            items.append((new_key, v))
    return dict(items)

def print_award_comparison(results: Dict[str, Any]) -> None:
    print("\nMatching fields:")
    for match in results["matches"]:
        print(f"{match['field']}: {match['value']}")

    print("\nMismatched fields:")
    for mismatch in results["mismatches"]:
        print(f"{mismatch['field']}:")
        print(f"  HigherGov: {mismatch['higher_gov_value']}")
        print(f"  Supabase: {mismatch['supabase_value']}")

    print("\nUnmapped HigherGov fields:")
    for field in results["unmapped_higher_gov_fields"]:
        print(f"{field['field']}: {field['value']}")

    print("\nUnmapped Supabase fields:")
    for field in results["unmapped_supabase_fields"]:
        print(f"{field['field']}: {field['value']}")

def run_award_comparison(api_key: str, supabase: Client) -> None:
    results = compare_award_data(
        api_key=api_key,
        supabase=supabase,
        award_id="15M10224PA4700443",
        piid="15M10224PA4700443"
    )
    print_award_comparison(results)

def run_opportunity_comparison(api_key: str, supabase: Client) -> None:
    results = compare_opportunity_data(
        api_key=api_key,
        supabase=supabase,
        solicitation_id="12024B23Q7001"
    )
    print("\nOpportunity Comparison Results:")
    print_award_comparison(results)

def get_filtered_opportunities(
    supabase: Client,
    active: bool = True,
    agencies: Optional[List[str]] = None,
    date_due_start: Optional[datetime] = None,
    date_due_end: Optional[datetime] = None,
    date_posted_start: Optional[datetime] = None,
    date_posted_end: Optional[datetime] = None,
    naics_codes: Optional[List[str]] = None,
    psc_codes: Optional[List[str]] = None,
    set_asides: Optional[List[str]] = None,
    page: int = 1,
    page_size: int = 20
) -> Dict[str, Any]:
    """
    Get filtered opportunities from the Notices table, only returning the latest notice for each solicitation.

    Args:
        supabase (Client): Initialized Supabase client
        active (bool): If True, only return opportunities with future response deadlines
        agencies (List[str], optional): List of agency names to filter by
        date_due_start (datetime, optional): Start of due date range
        date_due_end (datetime, optional): End of due date range
        date_posted_start (datetime, optional): Start of posted date range
        date_posted_end (datetime, optional): End of posted date range
        naics_codes (List[str], optional): List of NAICS codes to filter by
        psc_codes (List[str], optional): List of PSC codes to filter by
        set_asides (List[str], optional): List of set-aside types to filter by
        page (int): Page number for pagination (1-based)
        page_size (int): Number of records per page

    Returns:
        Dict containing:
            - count: Total number of matching records
            - data: List of opportunity records
            - page: Current page number
            - page_size: Number of records per page
            - total_pages: Total number of pages

    Raises:
        ValueError: If invalid filter parameters are provided
        Exception: For database errors
    """
    try:
        # Step 1: Apply filters to notices and get basic data (minimal fields)
        # Include naics and psc fields to use for verification later
        filter_query = supabase.from_("notices").select("""
            notice_id,
            solicitation_id,
            solicitation_response_deadline,
            naics,
            psc
        """)

        # Apply all filters
        if active:
            filter_query = filter_query.gt("solicitation_response_deadline", "now()")

        # Date filters
        if date_due_start:
            filter_query = filter_query.gte("solicitation_response_deadline", date_due_start.isoformat())
        if date_due_end:
            filter_query = filter_query.lte("solicitation_response_deadline", date_due_end.isoformat())
        if date_posted_start:
            filter_query = filter_query.gte("posted_date", date_posted_start.isoformat())
        if date_posted_end:
            filter_query = filter_query.lte("posted_date", date_posted_end.isoformat())

        # NAICS and PSC code filters - Apply directly to text columns
        if naics_codes:
            filter_query = filter_query.in_("naics", naics_codes)
        if psc_codes:
            filter_query = filter_query.in_("psc", psc_codes)

        # Set-aside filter
        if set_asides:
            filter_query = filter_query.in_("solicitation_set_aside", set_asides)

        # Agency filters
        if agencies:
            agency_conditions = []
            for agency in agencies:
                for i in range(1, 8):
                    agency_conditions.append(f"organization_level_{i}_name.ilike.%{agency}%")
            if agency_conditions:
                filter_query = filter_query.or_(agency_conditions)

        # Notice type filters
        filter_query = filter_query.in_("type", ["o", "p", "k", "r", "i"])
        filter_query = filter_query.not_.in_("type", ["a", "s", "j", "g", "f", "u", "v", "z"])

        # Execute the filtering query
        filtered_notices = filter_query.execute()

        if not filtered_notices.data:
            return {
                "count": 0,
                "data": [],
                "page": page,
                "page_size": page_size,
                "total_pages": 0
            }

        # Verify that the notices actually match our NAICS/PSC criteria
        # This is necessary because sometimes the filter doesn't work perfectly
        if naics_codes or psc_codes:
            verified_notices = []
            for notice in filtered_notices.data:
                # Check if NAICS code matches (if we're filtering by NAICS)
                naics_match = not naics_codes or (notice.get("naics") in naics_codes)
                # Check if PSC code matches (if we're filtering by PSC)
                psc_match = not psc_codes or (notice.get("psc") in psc_codes)

                # Only include if both criteria match
                if naics_match and psc_match:
                    verified_notices.append(notice)

            # Replace the filtered notices with the verified ones
            filtered_notices.data = verified_notices

        if not filtered_notices.data:
            return {
                "count": 0,
                "data": [],
                "page": page,
                "page_size": page_size,
                "total_pages": 0
            }

        # Step 2: Get unique solicitation IDs from filtered notices
        solicitation_ids = list(set(notice["solicitation_id"] for notice in filtered_notices.data
                                  if notice.get("solicitation_id")))

        if not solicitation_ids:
            return {
                "count": 0,
                "data": [],
                "page": page,
                "page_size": page_size,
                "total_pages": 0
            }

        # Step 3: Get latest notice IDs for these solicitations
        latest_notices_response = supabase.from_("solicitations") \
            .select("solicitation_id, latest_notice_id") \
            .in_("solicitation_id", solicitation_ids) \
            .execute()

        # Map solicitation_id to latest_notice_id
        latest_notice_map = {item["solicitation_id"]: item["latest_notice_id"]
                           for item in latest_notices_response.data if item.get("latest_notice_id")}

        # Extract all latest notice IDs
        latest_notice_ids = list(latest_notice_map.values())

        if not latest_notice_ids:
            return {
                "count": 0,
                "data": [],
                "page": page,
                "page_size": page_size,
                "total_pages": 0
            }

        # Step 4: Find notices that are both filtered and the latest
        filtered_and_latest = [notice for notice in filtered_notices.data
                              if notice["notice_id"] in latest_notice_ids]

        total_count = len(filtered_and_latest)
        total_pages = (total_count + page_size - 1) // page_size

        # Calculate pagination indexes
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, total_count)

        # Get the notice IDs for this page
        page_notice_ids = [notice["notice_id"] for notice in filtered_and_latest[start_idx:end_idx]]

        if not page_notice_ids:
            return {
                "count": total_count,
                "data": [],
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages
            }

        # Step 5: Get full details of the paged notices
        detailed_query = supabase.from_("notices").select("""
            *,
            naics_details:naics!naics_id (
                naics_id,
                naics_code,
                naics_title,
                naics_size
            ),
            psc_details:psc!psc_id (
                psc_id,
                psc_code,
                psc_name,
                psc_full_name
            ),
            set_aside_details:setasides!set_aside_id (
                set_aside_id,
                set_aside_code,
                set_aside_name
            ),
            organization:organizations!Notices_organization_key_fkey (
                organization_key,
                name,
                type,
                full_parent_path,
                full_parent_path_name
            ),
            organization_address:addresses!organization_address_key (
                address_key,
                street_address,
                street_address_2,
                city,
                state,
                zipcode,
                country_code
            ),
           
        """).in_("notice_id", page_notice_ids)

        # Order by due date
        detailed_query = detailed_query.order("solicitation_response_deadline", desc=False)

        # Get the final results
        detailed_response = detailed_query.execute()

        # Final verification on the detailed data to ensure it matches our criteria
        if naics_codes or psc_codes:
            verified_detailed_data = []
            for notice in detailed_response.data:
                # Check for NAICS match
                naics_match = True
                if naics_codes:
                    notice_naics = notice.get("naics")
                    naics_match = notice_naics in naics_codes if notice_naics else False

                # Check for PSC match
                psc_match = True
                if psc_codes:
                    notice_psc = notice.get("psc")
                    psc_match = notice_psc in psc_codes if notice_psc else False

                # Include only if both match
                if naics_match and psc_match:
                    verified_detailed_data.append(notice)
                else:
                    print(f"INFO: Filtered out notice {notice.get('notice_id')} with NAICS: {notice.get('naics')} and PSC: {notice.get('psc')}")

            # Update count based on verified data
            actual_count = len(verified_detailed_data)
            actual_pages = (actual_count + page_size - 1) // page_size if actual_count > 0 else 0

            return {
                "count": actual_count,
                "data": verified_detailed_data,
                "page": page,
                "page_size": page_size,
                "total_pages": actual_pages
            }

        return {
            "count": total_count,
            "data": detailed_response.data,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages
        }

    except Exception as e:
        print(f"Error fetching opportunities: {str(e)}")
        raise

def get_all_filtered_opportunities(
    supabase: Client,
    active: bool = True,
    agencies: Optional[List[str]] = None,
    date_due_start: Optional[datetime] = None,
    date_due_end: Optional[datetime] = None,
    date_posted_start: Optional[datetime] = None,
    date_posted_end: Optional[datetime] = None,
    naics_codes: Optional[List[str]] = None,
    psc_codes: Optional[List[str]] = None,
    set_asides: Optional[List[str]] = None,
    page_size: int = 100,
    max_pages: Optional[int] = None
) -> Dict[str, Any]:
    """
    Get all pages of filtered opportunities from the Notices table.

    Args:
        ... (same as get_filtered_opportunities) ...
        page_size (int): Number of records per page (default 100)
        max_pages (Optional[int]): Maximum number of pages to fetch (None for all)

    Returns:
        Dict containing:
            - count: Total number of matching records
            - data: List of all opportunity records across all pages
            - pages_retrieved: Number of pages retrieved
            - total_pages: Total number of available pages
    """
    # Get first page and total count
    first_page = get_filtered_opportunities(
        supabase=supabase,
        active=active,
        agencies=agencies,
        date_due_start=date_due_start,
        date_due_end=date_due_end,
        date_posted_start=date_posted_start,
        date_posted_end=date_posted_end,
        naics_codes=naics_codes,
        psc_codes=psc_codes,
        set_asides=set_asides,
        page=1,
        page_size=page_size
    )

    all_opportunities = first_page['data']
    total_pages = first_page['total_pages']
    total_count = first_page['count']

    # Determine how many pages to fetch
    pages_to_fetch = min(total_pages, max_pages or total_pages)

    # Fetch remaining pages
    for page in range(2, pages_to_fetch + 1):
        print(f"Fetching page {page} of {pages_to_fetch}...")

        page_data = get_filtered_opportunities(
            supabase=supabase,
            active=active,
            agencies=agencies,
            date_due_start=date_due_start,
            date_due_end=date_due_end,
            date_posted_start=date_posted_start,
            date_posted_end=date_posted_end,
            naics_codes=naics_codes,
            psc_codes=psc_codes,
            set_asides=set_asides,
            page=page,
            page_size=page_size
        )

        all_opportunities.extend(page_data['data'])

    return {
        "count": total_count,
        "data": all_opportunities,
        "pages_retrieved": pages_to_fetch,
        "total_pages": total_pages
    }

def main():
    load_dotenv()

    HIGHERGOV_KEY : str = os.getenv('HIGHERGOV_KEY')
    SUPABASE_URL  : str = os.getenv("SUPABASE_URL")
    SUPABASE_KEY  : str = os.getenv('SUPABASE_KEY')

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    naics_codes = ["335312", "811111", "562910"]
    psc_codes = None # ["4810"]

    # Use get_all_filtered_opportunities to fetch all pages
    opportunities = get_all_filtered_opportunities(
        supabase=supabase,
        active=True,
        naics_codes=naics_codes,
        psc_codes=psc_codes,
        page_size=100,  # 100 records per page
        max_pages=None  # Get all pages
    )

    print(json.dumps(opportunities['data'], indent=2))
    print(f"Found {opportunities['count']} total opportunities")
    print(f"Retrieved {len(opportunities['data'])} opportunities across {opportunities['pages_retrieved']} pages")
    print(f"Total available pages: {opportunities['total_pages']}")

if __name__ == "__main__":
    main()