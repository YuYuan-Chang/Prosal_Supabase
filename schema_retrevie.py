import json

def load_highergov_opportunities(filepath):
    """
    Load the HigherGov API file and extract opportunity identifiers.
    Each opportunity is represented as a set containing:
      - source_id
      - source_id_version
    Returns a list of sets.
    """
    with open(filepath, "r") as f:
        data = json.load(f)
        # Determine if data is wrapped in a dict or is a list of records.
        if isinstance(data, dict):
            records = data.get("results", [])
        elif isinstance(data, list):
            records = data
        else:
            records = []
    opportunities = []
    for record in records:
        ids = set()
        if "source_id" in record:
            ids.add(record["source_id"])
        if "source_id_version" in record:
            ids.add(record["source_id_version"])
        opportunities.append(ids)
    return opportunities

def load_results_opportunities(filepath):
    """
    Load the results JSON file and extract opportunity identifiers.
    Each opportunity is represented as a set containing:
      - solicitation_id (if it exists)
      - solicitationNumber from any history records (if available)
    Returns a list of sets.
    """
    with open(filepath, "r") as f:
        data = json.load(f)
    opportunities = []
    for record in data:
        ids = set()
        if "solicitation_id" in record:
            ids.add(record["solicitation_id"])
        if "history" in record:
            for hist in record["history"]:
                if "solicitationNumber" in hist:
                    ids.add(hist["solicitationNumber"])
        opportunities.append(ids)
    return opportunities

def find_match(opportunity, other_opportunities):
    """
    Given a set representing an opportunity, check if there is any opportunity in other_opportunities
    which has a non-empty intersection with it.
    If so, the opportunity is considered to have a match.
    """
    for other in other_opportunities:
        if opportunity.intersection(other):
            return True
    return False

def main():
    highergov_file = "highergov_api_full_data.json"
    results_file = "results.json"

    # Load opportunities from both files.
    highergov_ops = load_highergov_opportunities(highergov_file)
    results_ops = load_results_opportunities(results_file)
    print("Highergov opportunities:", highergov_ops)
    
    # Determine mismatches by checking each opportunity against the other file.
    mismatch_records_highergov = [op for op in highergov_ops if not find_match(op, results_ops)]
    mismatches_highergov = len(mismatch_records_highergov)
    
    mismatch_records_results = [op for op in results_ops if not find_match(op, highergov_ops)]
    mismatches_results = len(mismatch_records_results)
    
    total_mismatches = mismatches_highergov + mismatches_results

    print("----- Mismatch Report -----")
    print("Number of entries from highergov_api_full_data.json:", len(highergov_ops))
    print("Number of entries from results.json:", len(results_ops))
    print("Number of opportunities in highergov_api_full_data.json with no match in results.json:", mismatches_highergov)
    print("Number of opportunities in results.json with no match in highergov_api_full_data.json:", mismatches_results)
    print("Total mismatches:", total_mismatches)
    
    # Print out the mismatch details
    print("\nDetailed Mismatches:")
    print("Mismatched opportunities from highergov_api_full_data.json:")
    for op in mismatch_records_highergov:
        print(op)
    
    print("\nMismatched opportunities from results.json:")
    for op in mismatch_records_results:
        print(op)

if __name__ == "__main__":
    main()