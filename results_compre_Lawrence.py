import csv
import json

def load_csv_opportunities(filepath):
    """
    Load the contract opportunity CSV file and extract opportunity identifiers.
    Each opportunity is represented as a set containing:
      - source_id
      - source_id_version
    Returns a list of sets.
    """
    opportunities = []
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            #print(row)
            ids = set()
            # Check and add identifier fields if present and non-empty.
            if 'Solicitation ID' in row and row['Solicitation ID']:
                ids.add(row['Solicitation ID'])
            if 'Solicitation Title' in row and row['Solicitation Title']:
                ids.add(row['Solicitation Title'])
            if '\ufeffNotice ID' in row and row['\ufeffNotice ID']:
                #print("catch")
                ids.add(row['\ufeffNotice ID'])
            opportunities.append(ids)
    return opportunities

def load_results_opportunities(filepath):
    """
    Load the supabase JSON file and extract opportunity identifiers.
    Each opportunity is represented as a set containing:
      - solicitation_id (if it exists)
      - notice_id (if it exists)
      - solicitationNumber from any history records (if available)
    Returns a list of sets.
    """
    with open(filepath, "r") as f:
        data = json.load(f)
    opportunities = []
    for record in data:
        ids = set()
        if "solicitation_id" in record and record["solicitation_id"]:
            ids.add(record["solicitation_id"])
        if "notice_id" in record and record["notice_id"]:
            ids.add(record["notice_id"])
        if "title" in record and record["title"]:
            ids.add(record["title"])
        if "history" in record:
            for hist in record["history"]:
                if "solicitationNumber" in hist and hist["solicitationNumber"]:
                    ids.add(hist["solicitationNumber"])
        opportunities.append(ids)
    return opportunities

def find_match(opportunity, other_opportunities):
    """
    Given a set representing an opportunity, check if there is any opportunity in other_opportunities
    that has a non-empty intersection with it.
    If so, the opportunity is considered to have a match.
    """
    for other in other_opportunities:
        if opportunity.intersection(other):
            return True
    return False

def main():
    csv_file = "contract_opportunity-03-17-25-18-48-13.csv"
    results_file = "wnzTPS5NfNK5vVqRhbQ9i_results.json"

    # Load opportunities from both files.
    csv_ops = load_csv_opportunities(csv_file)
    results_ops = load_results_opportunities(results_file)
    #print("CSV opportunities:", csv_ops)
    
    # Determine mismatches by checking each opportunity against the other file.
    mismatch_records_csv = [op for op in csv_ops if not find_match(op, results_ops)]
    mismatches_csv = len(mismatch_records_csv)
    
    mismatch_records_results = [op for op in results_ops if not find_match(op, csv_ops)]
    mismatches_results = len(mismatch_records_results)
    
    total_mismatches = mismatches_csv + mismatches_results

    print("----- Mismatch Report -----")
    print("Number of entries from CSV file:", len(csv_ops))
    print("Number of entries from results.json:", len(results_ops))
    print("Opportunities in highergov CSV with no match in supabase:", mismatches_csv)
    print("Opportunities in supabase with no match in highergov CSV:", mismatches_results)
    print("Total mismatches:", total_mismatches)
    
    # Print out the mismatch details
    print("\nDetailed Mismatches:")
    print("Mismatched opportunities from CSV file:")
    for op in mismatch_records_csv:
        print(op)
    
    print("\nMismatched opportunities from results.json:")
    for op in mismatch_records_results:
        print(op)

if __name__ == "__main__":
    main()