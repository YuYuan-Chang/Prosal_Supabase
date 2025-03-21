import os
import json
from datetime import datetime, timezone, date, timedelta
from typing import List, Dict, Optional
from supabase import create_client, Client
from dotenv import load_dotenv
from dataclasses import dataclass

@dataclass
class AwardsQuery:
    INCLUDE_RECIPIENT_UEI: Optional[List[str]] = None
    EXCLUDE_RECIPIENT_UEI: Optional[List[str]] = None
    POTENTIAL_END_DATE_START: Optional[str] = None
    POTENTIAL_END_DATE_END: Optional[str] = None
    INCLUDE_NAICS: Optional[List[str]] = None
    EXCLUDE_NAICS: Optional[List[str]] = None
    INCLUDE_PSC: Optional[List[str]] = None
    EXCLUDE_PSC: Optional[List[str]] = None
    INCLUDE_SET_ASIDE_IDS: Optional[List[str]] = None
    EXCLUDE_SET_ASIDE_IDS: Optional[List[str]] = None
    INCLUDE_ORGANIZATION_KEYS: Optional[List[str]] = None
    EXCLUDE_ORGANIZATION_KEYS: Optional[List[str]] = None
    INCLUDE_EXTENT_COMPETED: Optional[List[str]] = None
    EXCLUDE_EXTENT_COMPETED: Optional[List[str]] = None
    AMOUNT_OBLIGATED_MINIMUM: Optional[float] = None
    AMOUNT_OBLIGATED_MAXIMUM: Optional[float] = None
    KEYWORD_QUERY: Optional[str] = None


def get_filtered_awards(supabase: Client, aq: AwardsQuery) -> List[Dict[str, any]]:
    all_awards = []
    limit = 1000
    offset = 0

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
        query = supabase.from_("awards").select("*")
        
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
        
        if aq.POTENTIAL_END_DATE_START:
            print("Applying POTENTIAL_END_DATE_START filter with value:", aq.POTENTIAL_END_DATE_START)
            query = query.gte("period_of_performance_potential_end_date", aq.POTENTIAL_END_DATE_START)
        if aq.POTENTIAL_END_DATE_END:
            print("Applying POTENTIAL_END_DATE_END filter with value:", aq.POTENTIAL_END_DATE_END)
            query = query.lte("period_of_performance_potential_end_date", aq.POTENTIAL_END_DATE_END)
        
        if aq.INCLUDE_NAICS:
            print("Applying INCLUDE_NAICS filter with values:", aq.INCLUDE_NAICS)
            query = query.in_("naics", aq.INCLUDE_NAICS)
        if aq.EXCLUDE_NAICS:
            for code in aq.EXCLUDE_NAICS:
                print("Excluding NAICS code:", code)
                query = query.neq("naics", code)
        
        if aq.INCLUDE_PSC:
            print("Applying INCLUDE_PSC filter with values:", aq.INCLUDE_PSC)
            query = query.in_("product_or_service_code", aq.INCLUDE_PSC)
        if aq.EXCLUDE_PSC:
            for psc in aq.EXCLUDE_PSC:
                print("Excluding PSC code:", psc)
                query = query.neq("product_or_service_code", psc)
        
        if aq.INCLUDE_SET_ASIDE_IDS:
            print("Applying INCLUDE_SET_ASIDE_IDS filter with values:", aq.INCLUDE_SET_ASIDE_IDS)
            query = query.in_("type_set_aside", aq.INCLUDE_SET_ASIDE_IDS)
        if aq.EXCLUDE_SET_ASIDE_IDS:
            for sid in aq.EXCLUDE_SET_ASIDE_IDS:
                print("Excluding SET_ASIDE_ID:", sid)
                query = query.neq("type_set_aside", sid)
        
        if fpds_codes_include:
            print("Applying organization include filter with fpds_codes:", fpds_codes_include)
            query = query.in_("funding_agency_subtier_agency_code", fpds_codes_include)
        if fpds_codes_exclude:
            for code in fpds_codes_exclude:
                print("Excluding organization fpds_code:", code)
                query = query.neq("funding_agency_subtier_agency_code", code)
        
        if aq.INCLUDE_EXTENT_COMPETED:
            print("Applying INCLUDE_EXTENT_COMPETED filter with values:", aq.INCLUDE_EXTENT_COMPETED)
            query = query.in_("extent_competed_description", aq.INCLUDE_EXTENT_COMPETED)
        if aq.EXCLUDE_EXTENT_COMPETED:
            for val in aq.EXCLUDE_EXTENT_COMPETED:
                print("Excluding EXTENT_COMPETED value:", val)
                query = query.neq("extent_competed_description", val)
        
        if aq.AMOUNT_OBLIGATED_MINIMUM is not None:
            print("Applying AMOUNT_OBLIGATED_MINIMUM filter with value:", aq.AMOUNT_OBLIGATED_MINIMUM)
            query = query.gte("total_obligation", aq.AMOUNT_OBLIGATED_MINIMUM)
        if aq.AMOUNT_OBLIGATED_MAXIMUM is not None:
            print("Applying AMOUNT_OBLIGATED_MAXIMUM filter with value:", aq.AMOUNT_OBLIGATED_MAXIMUM)
            query = query.lte("total_obligation", aq.AMOUNT_OBLIGATED_MAXIMUM)

        query = query.range(offset, offset + limit - 1)
        
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

    aq = AwardsQuery(
        INCLUDE_NAICS=["541715", "541511", "541512", "541519", "541330", "928110"],
        POTENTIAL_END_DATE_START=date.today().isoformat(),
        POTENTIAL_END_DATE_END=(date.today() + timedelta(days=18*30)).isoformat(),
        EXCLUDE_EXTENT_COMPETED=["NOT AVAILABLE FOR COMPETITION", "NOT COMPETED"],
        AMOUNT_OBLIGATED_MINIMUM=250000,
        KEYWORD_QUERY="'System Platform Modernization' | 'Data Management Modernization' | 'Digital Engineering' | " \
                     "'Human Machine Interaction' | 'Customized Software Development' | 'Secure Computer Intelligence' | " \
                     "'Cyber Physical Hardware security' | 'Cyber Automation Vulnerability Exploitation' | 'Cyber engineering' | " \
                     "'microelectronics Analysis' | 'Microelectronics prototypes' | 'Micro electronics development' | " \
                     "'Combat Identification' | 'Validated Signatures Database' | 'Data fusion' | 'Algorithm Development' | " \
                     "'Synthetic Modeling' | 'Data Production' | 'C4ISR' | 'Joint Multiplatform Advanced CID' | 'JMAC' | " \
                     "'Streamlined rapid reprogramming' | 'FME' | 'HWRE' | 'SWRE' | 'MS models' | 'tools' | 'architecture' | " \
                     "'MSA' | 'AFSIM' | 'MANPADS' | 'emulators' | 'EW' | 'EP' | 'EA' | 'ES' | 'radar' | 'All-Source Analysis' | " \
                     "'C3' | 'C4' | 'Mosaic Warfare' | 'kill web' | 'kill chain' | 'T&E' | 'Systems design' | " \
                     "'Payload development' | 'RF Engineering' | 'SW engineering development' | 'APNT' | 'physics-based MSA' | " \
                     "'AI ML Edge Processing Prototyping' | 'counter-unmanned aerial systems' | " \
                     "'electro optic infrared tracking' | 'counter cruise missile' | " \
                     "'directed energy weapon system capabilities modeling' | 'Model-Based System Engineering' | 'Architectures' | " \
                     "'testing fielding innovative technology quickly' | 'Independent Verification Validation' | 'Threat Emulator' | " \
                     "'Verification Validation' | 'Systems Engineering' | 'Prototyping' | 'Software Development' | " \
                     "'Test Evaluation' | 'Modernization Upgrades Directed Energy T&E' | 'Hardware Software Integration' | " \
                     "'Army Airworthiness Expertise' | 'Aeromechanics Subject Matter Expertise' | 'Army ISR Expertise' | " \
                     "'Sensor Technology Research Integration Test Evaluation' | 'ISR Sensor Simulation Emulation' | " \
                     "'Training Systems Cyber RMF' | 'TENA Expertise' | 'Software Modernization' | 'Shared Services' | " \
                     "'System Modernization' | 'Platform Modernization' | 'Data Management' | 'Digital Engineering' | " \
                     "'Human Machine Interaction' | 'Zero Trust' | 'Cloud' | 'Anti-Tamper Cyber Defense' | " \
                     "'Microelectronics Exploitation' | 'Cyber Vulnerability Assessments' | 'Protective Technology' | " \
                     "'C4ISR Analysis' | 'Physics MS' | 'AI ML' | 'Research and Development' | 'Intel Analysis' | " \
                     "'Reverse Engineering Exploitation' | 'Algorithm Dev Analysis' | 'Foreign Materiel Exploitation' | " \
                     "'Hardware Reverse Engineering' | 'Software Reverse Engineering' | 'Reverse Engineering' | " \
                     "'Field Programmable Gate Array' | 'Complex Device' | 'exploitation' | 'Modeling Simulation' | " \
                     "'Modeling' | 'Simulation' | 'Analysis' | 'Scientific Technical Intelligence' | 'all-source analysis' | " \
                     "'radar' | 'missile' | 'electronic warfare' | 'open source intelligence' | 'Publicly Available Information' | " \
                     "'electronic intelligence' | 'communication intelligence' | 'human intelligence' | 'signals intelligence' | " \
                     "'Foreign Instrumentation Signal Intelligence' | 'FIS' | 'measurement signature intelligence' | 'AFSIM' | " \
                     "'SAFE-SiM' | 'TMAP' | 'NGTS' | 'ITEAMS' | 'Joint Simulation Environment' | 'Overhead Persistent Infrared' | " \
                     "'electro-optical' | 'infrared' | 'manportable air defense system' | 'radio frequency' | " \
                     "'imagery intelligence' | 'geospatial intelligence' | 'characteristics performance' | 'translation' | " \
                     "'Integrated Threat Analysis Simulation Environment' | 'Threat Modeling Assessment Program' | " \
                     "'Next Generation Threat Simulation' | 'verification validation' | 'Command Control' | 'Command' | 'Control' | " \
                     "'Communication' | 'Command' | 'Control' | 'Communication' | 'Computer' | 'Command' | 'Control' | " \
                     "'Communication' | 'Computer Cyber' | 'Intelligence, Surveillance, Reconnaissance' | 'C3ISR' | 'C4ISR' | " \
                     "'electronic countermeasures' | 'electronic counter-countermeasures' | 'countermeasures' | " \
                     "'electronic protection' | 'electronic attack' | 'electronic support' | 'electromagnetic warfare' | " \
                     "'tactics techniques procedures' | 'cognitive EW' | 'test evaluation' | " \
                     "'Laboratory Intelligence Validated Emulator' | 'Signature Surrogates' | 'Ground-Based Strategic Deterrent' | " \
                     "'Mosaic Warfare' | 'kill web' | 'kill chain' | 'Payload development' | 'APNT' | 'AI/ML Edge Processing' | " \
                     "'Prototyping' | 'laser' | 'laser weapon' | 'high power microwave' | 'radio frequency weapon' | " \
                     "'directed energy' | 'directed energy weapon' | 'C-UAS' | 'C-CM' | 'power thermal technologies' | " \
                     "'electro-optic' | 'infrared' | 'Systems Engineering Technical Advice' | 'Systems Engineering' | " \
                     "'prototyping' | 'weapon system analysis' | 'Independent Verification Validation' | 'System Integration' | " \
                     "'Test Evaluation Upgrades Modernization' | 'Training Systems Devices' | " \
                     "'Aviation Platform Sustainment Enhancements' | 'Army Intelligence Analysis ISR Modernization' | " \
                     "'Systems Engineering' | 'Prototyping' | 'Directed Energy' | 'Reagan Test Site' | 'Airworthiness' | " \
                     "'Aeromechanics' | 'Software Development' | 'COTS Hardware Integration' | 'Live Virtual Constructive' | " \
                     "'Test Training Enabling Architecture' | 'MTRB IM' | 'EO IR SAR MTI Sensor Emulation Simulation' | " \
                     "'Contractor Logistic Support' | 'Cyber RMF'"
    )

    awards = get_filtered_awards(supabase, aq)
    print("Number of awards results:", len(awards))
    with open("results/awards_filtered_results.json", "w") as f:
        json.dump(awards, f, indent=2, default=str)
    print("Saved results to results/awards_filtered_results.json")


if __name__ == "__main__":
    main()