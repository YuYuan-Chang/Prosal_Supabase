import os
import re
import json
from typing import List, Dict, Optional
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timezone

def get_filtered_notices(
    supabase: Client,
    active: bool = True,
    include_naics: Optional[List[str]] = None,
    exclude_naics: Optional[List[str]] = None,
    include_solicitation_types: Optional[List[str]] = None,
    exclude_solicitation_types: Optional[List[str]] = None,
    include_psc: Optional[List[str]] = None,
    exclude_psc: Optional[List[str]] = None,
    include_set_aside_ids: Optional[List[str]] = None,  # new filter: include by set_aside_id
    exclude_set_aside_ids: Optional[List[str]] = None,  # new filter: exclude by set_aside_id
    include_organization_keys: Optional[List[str]] = None,  # new filter: include by organization_key (int8)
    exclude_organization_keys: Optional[List[str]] = None,
    keyword_query: Optional[str] = None
) -> List[Dict[str, any]]:
    """
    Retrieve rows from the 'notices' table applying the provided filters and paginating through all the available data.
    """
    all_notices = []
    limit = 1000  # Batch size for pagination.
    offset = 0
    

    while True:
        print("offset: ", offset)
        # Build the base query with embedded joins.
        query = supabase.from_("notices").select("""
            *,
            naics_details:naics!naics_id(*),
            psc_details:psc!psc_id(*),
            setasides_details:setasides!set_aside_id(*),
            solicitations_details:solicitations!solicitation_id(*),
            addresses_details:addresses!organization_address_key(*),
            organization_details:organizations!Notices_organization_key_fkey(*),
            organization_level_1_details:organizations!Notices_organization_level_1_key_fkey(*),
            organization_level_2_details:organizations!Notices_organization_level_2_key_fkey(*),
            organization_level_3_details:organizations!Notices_organization_level_3_key_fkey(*),
            organization_level_4_details:organizations!Notices_organization_level_4_key_fkey(*),
            organization_level_5_details:organizations!Notices_organization_level_5_key_fkey(*),
            organization_level_6_details:organizations!Notices_organization_level_6_key_fkey(*),
            organization_level_7_details:organizations!Notices_organization_level_7_key_fkey(*),
            solicitation_type_details:solicitation_types!type(*)
        """)

        if active:
            print("Applying active filter")
            query = query.eq("latest", True)
            current_time = datetime.now(timezone.utc).isoformat()
            print("Current time:", current_time)
            # Only include notices with a future solicitation_response_deadline.
            query = query.gt("solicitation_response_deadline", current_time)

        if include_naics:
            print("Including NAICS codes:", include_naics)
            query = query.in_("naics", include_naics)
        if exclude_naics:
            for code in exclude_naics:
                query = query.neq("naics", code)

        if include_solicitation_types:
            query = query.in_("type", include_solicitation_types)
        if exclude_solicitation_types:
            print("Excluding solicitation types:", exclude_solicitation_types)
            for s_type in exclude_solicitation_types:
                query = query.neq("type", s_type)

        if include_psc:
            query = query.in_("psc", include_psc)
        if exclude_psc:
            for psc in exclude_psc:
                query = query.neq("psc", psc)

        if include_set_aside_ids:
            # Convert set_aside_ids to integers for matching database type.
            include_set_aside_ids = [int(x) for x in include_set_aside_ids]
            print("Including set_aside_ids:", include_set_aside_ids)
            query = query.in_("set_aside_id", include_set_aside_ids)
        if exclude_set_aside_ids:
            # Convert set_aside_ids to integers for matching database type.
            exclude_set_aside_ids = [int(x) for x in exclude_set_aside_ids]
            print("Excluding set_aside_ids:", exclude_set_aside_ids)
            for set_aside_id in exclude_set_aside_ids:
                query = query.neq("set_aside_id", set_aside_id)

        if include_organization_keys:
            # Convert organization_keys to integers for matching database type.
            include_organization_keys = [int(x) for x in include_organization_keys]
            print("Including organization keys across multiple columns:", include_organization_keys)
            conditions = []
            for key in include_organization_keys:
                conditions.append(f"organization_key.eq.{key}")
                conditions.append(f"organization_level_1_key.eq.{key}")
                conditions.append(f"organization_level_2_key.eq.{key}")
                conditions.append(f"organization_level_3_key.eq.{key}")
                conditions.append(f"organization_level_4_key.eq.{key}")
                conditions.append(f"organization_level_5_key.eq.{key}")
                conditions.append(f"organization_level_6_key.eq.{key}")
                conditions.append(f"organization_level_7_key.eq.{key}")
            or_filter = ",".join(conditions)
            query = query.or_(or_filter)

        if exclude_organization_keys:
            # Convert organization_keys to integers for matching database type.
            exclude_organization_keys = [int(x) for x in exclude_organization_keys]
            print("Excluding organization_keys:", exclude_organization_keys)
            for org_key in exclude_organization_keys:
                query = query.neq("organization_key", org_key)

        query = query.range(offset, offset + limit - 1)
        
        if keyword_query:
            print("Applying keyword search filter:", keyword_query)
            # Use full-text search on the "opportunity_text" field.
            query = query.text_search("opportunity_text", keyword_query, {'config': 'english', 'type': 'websearch'})
            
        result = query.execute()
        
        # Exit loop if no more data is returned.
        if not result.data:
            break
        

        all_notices.extend(result.data)
        offset += limit

    return all_notices

def main():
    load_dotenv()
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables must be set")

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Define filter parameters.
    active = True
    include_naics = [
        "237310", "488111", "493110", "518210", "541330", "541511", "541512", "541519",
        "541611", "541614", "562910", "513210", "516210", "561210", "221330", "236210",
        "236220", "237110", "237990", "238110", "238350", "238910", "238990", "333922",
        "541219", "541350", "541360", "541370", "541380", "541430", "541513", "541612",
        "541618", "541620", "541690", "541713", "541714", "541715", "541810", "541820",
        "541830", "541840", "541850", "541860", "541870", "541890", "541990", "561410",
        "561421", "561499", "561730", "561790", "611430"
    ]
    exclude_naics = []

    include_solicitation_types = []  # types to include
    exclude_solicitation_types = ["s", "a", "u", "j", "l", "m", "g", "f"]  # types to exclude
    
    include_psc = []
    exclude_psc = []
    
    # Filters for set aside codes and IDs.
    include_set_aside_ids = []  
    exclude_set_aside_ids = [16, 24, 17, 18, 19, 20, 22, 21, 23]  # example set_aside_id values to exclude

    # filters for organization keys (int8).
    # Filter for organization keys across multiple columns. 
    # When a key is provided, notices with that key in any of the following columns will be returned:
    # organization_key, organization_level_1_key, organization_level_2_key, organization_level_3_key,
    # organization_level_4_key, organization_level_5_key, organization_level_6_key, organization_level_7_key.
    
    include_organization_keys = [300000201]  # example organization keys to include (int8)
    exclude_organization_keys = []  # example organization keys to exclude (int8)


    #keyword_query = "'ITAD' | 'media' | 'destruction' | 'digital' | 'media' | 'destruction'"
    keyword_query = ""

    # Retrieve notices using the specified filters.
    notices = get_filtered_notices(
        supabase=supabase,
        active=active,
        include_naics=include_naics,
        exclude_naics=exclude_naics,
        include_solicitation_types=include_solicitation_types,
        exclude_solicitation_types=exclude_solicitation_types,
        include_psc=include_psc,
        exclude_psc=exclude_psc,
        include_set_aside_ids=include_set_aside_ids,
        exclude_set_aside_ids=exclude_set_aside_ids,
        include_organization_keys=include_organization_keys,
        exclude_organization_keys=exclude_organization_keys,
        keyword_query=keyword_query
    )

    print("Number of results:", len(notices))
    with open("wnzTPS5NfNK5vVqRhbQ9i_results.json", "w") as f:
        json.dump(notices, f, indent=2, default=str)
    print("Saved results to wnzTPS5NfNK5vVqRhbQ9i_result.json file")

if __name__ == "__main__":
    main()