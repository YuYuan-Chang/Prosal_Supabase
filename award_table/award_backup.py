import os
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional
from supabase import create_client, Client
from dotenv import load_dotenv
from dataclasses import dataclass

# This dataclass is already defined in your award_table_filter.py file.
@dataclass
class AwardsQuery:
    # The comments are hints for which fields to reference.
    INCLUDE_RECIPIENT_UEI: Optional[List[str]] = None  # also includes parent_recipient_uei
    EXCLUDE_RECIPIENT_UEI: Optional[List[str]] = None  # also includes parent_recipient_uei
    POTENTIAL_END_DATE_START: Optional[str] = None     # filter on period_of_performance_potential_end_date (>=)
    POTENTIAL_END_DATE_END: Optional[str] = None       # filter on period_of_performance_potential_end_date (<=)
    INCLUDE_NAICS: Optional[List[str]] = None          # filter on naics column
    EXCLUDE_NAICS: Optional[List[str]] = None          # filter on naics column
    INCLUDE_PSC: Optional[List[str]] = None            # filter on product_or_service_code column
    EXCLUDE_PSC: Optional[List[str]] = None            # filter on product_or_service_code column
    INCLUDE_SET_ASIDE_IDS: Optional[List[str]] = None   # filter on type_set_aside column
    EXCLUDE_SET_ASIDE_IDS: Optional[List[str]] = None   # filter on type_set_aside column
    INCLUDE_ORGANIZATION_KEYS: Optional[List[str]] = None  # use organization_key to look up organizations.fpds_code and then filter awards.funding_agency_subtier_agency_code
    EXCLUDE_ORGANIZATION_KEYS: Optional[List[str]] = None
    INCLUDE_EXTENT_COMPETED: Optional[List[str]] = None  # filter on extent_competed_description column
    EXCLUDE_EXTENT_COMPETED: Optional[List[str]] = None  # filter on extent_competed_description column
    AMOUNT_OBLIGATED_MINIMUM: Optional[float] = None     # filter on total_obligation (>=)
    AMOUNT_OBLIGATED_MAXIMUM: Optional[float] = None     # filter on total_obligation (<=)
    KEYWORD_QUERY: Optional[str] = None                # full-text search on description (for example)


def get_filtered_awards(supabase: Client, aq: AwardsQuery) -> List[Dict[str, any]]:
    """
    Retrieve rows from the 'awards' table applying the filters from AwardsQuery and joining related tables.
    """
    all_awards = []
    limit = 1000  # batch size for pagination
    offset = 0

    # ----- Organization keys filter: convert organization keys into corresponding fpds_codes -----
    fpds_codes_include = []
    if aq.INCLUDE_ORGANIZATION_KEYS:
        print("Applying INCLUDE_ORGANIZATION_KEYS filter with values:", aq.INCLUDE_ORGANIZATION_KEYS)
        org_res = supabase.from_("organizations")\
            .select("fpds_code")\
            .in_("organization_key", aq.INCLUDE_ORGANIZATION_KEYS)\
            .execute()
        if org_res.data:
            fpds_codes_include = [org["fpds_code"] for org in org_res.data if org.get("fpds_code")]
            print("Resolved fpds_codes for include:", fpds_codes_include)

    fpds_codes_exclude = []
    if aq.EXCLUDE_ORGANIZATION_KEYS:
        print("Applying EXCLUDE_ORGANIZATION_KEYS filter with values:", aq.EXCLUDE_ORGANIZATION_KEYS)
        org_res = supabase.from_("organizations")\
            .select("fpds_code")\
            .in_("organization_key", aq.EXCLUDE_ORGANIZATION_KEYS)\
            .execute()
        if org_res.data:
            fpds_codes_exclude = [org["fpds_code"] for org in org_res.data if org.get("fpds_code")]
            print("Resolved fpds_codes for exclude:", fpds_codes_exclude)

    while True:
        # Build the base query with joins to related tables.
        query = supabase.from_("awards").select("*")
        
        # Apply filter: include recipient_uei (checking both recipient_uei and parent_recipient_uei).
        if aq.INCLUDE_RECIPIENT_UEI:
            print("Applying INCLUDE_RECIPIENT_UEI filter with values:", aq.INCLUDE_RECIPIENT_UEI)
            uei_values = ",".join(aq.INCLUDE_RECIPIENT_UEI)
            condition = f"recipient_uei.in.({uei_values}),parent_recipient_uei.in.({uei_values})"
            query = query.or_(condition)
        
        if aq.EXCLUDE_RECIPIENT_UEI:
            for uei in aq.EXCLUDE_RECIPIENT_UEI:
                print("Excluding RECIPIENT_UEI:", uei)
                query = query.neq("recipient_uei", uei)
                query = query.neq("parent_recipient_uei", uei)
        
        # Filter on potential end date (assume period_of_performance_potential_end_date is a timestamp).
        if aq.POTENTIAL_END_DATE_START:
            print("Applying POTENTIAL_END_DATE_START filter with value:", aq.POTENTIAL_END_DATE_START)
            query = query.gte("period_of_performance_potential_end_date", aq.POTENTIAL_END_DATE_START)
        if aq.POTENTIAL_END_DATE_END:
            print("Applying POTENTIAL_END_DATE_END filter with value:", aq.POTENTIAL_END_DATE_END)
            query = query.lte("period_of_performance_potential_end_date", aq.POTENTIAL_END_DATE_END)
        
        # NAICS filters
        if aq.INCLUDE_NAICS:
            print("Applying INCLUDE_NAICS filter with values:", aq.INCLUDE_NAICS)
            query = query.in_("naics", aq.INCLUDE_NAICS)
        if aq.EXCLUDE_NAICS:
            for code in aq.EXCLUDE_NAICS:
                print("Excluding NAICS code:", code)
                query = query.neq("naics", code)
        
        # PSC filters (using product_or_service_code column)
        if aq.INCLUDE_PSC:
            print("Applying INCLUDE_PSC filter with values:", aq.INCLUDE_PSC)
            query = query.in_("product_or_service_code", aq.INCLUDE_PSC)
        if aq.EXCLUDE_PSC:
            for psc in aq.EXCLUDE_PSC:
                print("Excluding PSC code:", psc)
                query = query.neq("product_or_service_code", psc)
        
        # Set-aside filters (using type_set_aside column)
        if aq.INCLUDE_SET_ASIDE_IDS:
            print("Applying INCLUDE_SET_ASIDE_IDS filter with values:", aq.INCLUDE_SET_ASIDE_IDS)
            query = query.in_("type_set_aside", aq.INCLUDE_SET_ASIDE_IDS)
        if aq.EXCLUDE_SET_ASIDE_IDS:
            for sid in aq.EXCLUDE_SET_ASIDE_IDS:
                print("Excluding SET_ASIDE_ID:", sid)
                query = query.neq("type_set_aside", sid)
        
        # Organization filter: use the fpds_codes obtained from the organizations table to filter awards.
        if fpds_codes_include:
            print("Applying organization include filter with fpds_codes:", fpds_codes_include)
            query = query.in_("funding_agency_subtier_agency_code", fpds_codes_include)
        if fpds_codes_exclude:
            for code in fpds_codes_exclude:
                print("Excluding organization fpds_code:", code)
                query = query.neq("funding_agency_subtier_agency_code", code)
        
        # Extent competed filters
        if aq.INCLUDE_EXTENT_COMPETED:
            print("Applying INCLUDE_EXTENT_COMPETED filter with values:", aq.INCLUDE_EXTENT_COMPETED)
            query = query.in_("extent_competed_description", aq.INCLUDE_EXTENT_COMPETED)
        if aq.EXCLUDE_EXTENT_COMPETED:
            for val in aq.EXCLUDE_EXTENT_COMPETED:
                print("Excluding EXTENT_COMPETED value:", val)
                query = query.neq("extent_competed_description", val)
        
        # Amount obligated filters on total_obligation
        if aq.AMOUNT_OBLIGATED_MINIMUM is not None:
            print("Applying AMOUNT_OBLIGATED_MINIMUM filter with value:", aq.AMOUNT_OBLIGATED_MINIMUM)
            query = query.gte("total_obligation", aq.AMOUNT_OBLIGATED_MINIMUM)
        if aq.AMOUNT_OBLIGATED_MAXIMUM is not None:
            print("Applying AMOUNT_OBLIGATED_MAXIMUM filter with value:", aq.AMOUNT_OBLIGATED_MAXIMUM)
            query = query.lte("total_obligation", aq.AMOUNT_OBLIGATED_MAXIMUM)

        # Apply pagination.
        query = query.range(offset, offset + limit - 1)
        
        # Keyword search filter: perform full-text search on description (adjust field as needed)
        if aq.KEYWORD_QUERY:
            print("Applying KEYWORD_QUERY filter with value:", aq.KEYWORD_QUERY)
            query = query.text_search("description", aq.KEYWORD_QUERY, {'config': 'english', 'type': 'websearch'})
        
        result = query.execute()
        if not result.data:
            break
        all_awards.extend(result.data)
        offset += limit

    return all_awards


def main():
    load_dotenv()
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables must be set")

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Define your filter parameters – adjust these values as needed.
    aq = AwardsQuery(
        # Uncomment and set your filter parameters as needed:
         INCLUDE_RECIPIENT_UEI=["NJWUS8RJAGX8", "KHV5NML35K71", "N12JJJ4JEK67", "HHWGP4HH1Y68", "DMPAKJ9N9K66"],
         EXCLUDE_RECIPIENT_UEI=["XYZ789"],
         POTENTIAL_END_DATE_START="2025-01-01T00:00:00Z",
        # POTENTIAL_END_DATE_END="2025-12-31T23:59:59Z",
         INCLUDE_NAICS = [
        "311999",
        "221210",
        "531311",
        "541519",
        "524114",
        "541511",
        "621999",
        "424950",
        "561720",
        "561210",
        "561621",
        "339113",
        "541930",
        "541330",
        "518210",
        "623110",
        "311511",
        "238990",
        "492110",
        "339116",
        "621910",
        "561612",
        "561499",
        "511210"
    ],
        # EXCLUDE_NAICS=["236210"],
        INCLUDE_PSC = [
            "8945", "8940", "S111", "X1FZ", "6505", "7G20", "X1FA", "F115", "S209", "G009",
            "F108", "Q999", "8010", "S201", "Z1AA", "J012", "6515", "R608", "S216", "R702",
            "Q402", "C1LB", "8910", "Z2AA", "R602", "6520", "V225", "R430", "C219", "S201",
            "R408", "R499", "J066", "R613", "1680", "C212", "6515", "V121", "R499", "S206",
            "6515", "DA10", "7F20", "S201"
         ],

        # EXCLUDE_PSC=["7030"],
         INCLUDE_SET_ASIDE_IDS=["SBA", "SDVOSBC", "VSA"],
        # EXCLUDE_SET_ASIDE_IDS=["C"],
        INCLUDE_ORGANIZATION_KEYS=["100006568", "100006688", "100006689", "100006748", "100006749", "100006773", "100006809", "100006822", "100015299"],
        # EXCLUDE_ORGANIZATION_KEYS=[],
        # INCLUDE_EXTENT_COMPETED=["Full Competition"],
        # EXCLUDE_EXTENT_COMPETED=[],
        # AMOUNT_OBLIGATED_MINIMUM=100000.0,
        # AMOUNT_OBLIGATED_MAXIMUM=500000.0,
        # KEYWORD_QUERY="search terms"
    )

    awards = get_filtered_awards(supabase, aq)
    print("Number of awards results:", len(awards))
    with open("results/awards_filtered_results.json", "w") as f:
        json.dump(awards, f, indent=2, default=str)
    print("Saved results to awards_filtered_results.json")


if __name__ == "__main__":
    main()