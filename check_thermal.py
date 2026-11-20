import json
from collections import Counter

path = "result_data_welding/mechanical_results.jsonl"
keywords = ("thermal", "temperature", "temp", "heat", "stress")

counts = Counter()
examples = {}

with open(path, "r", encoding="utf-8") as file:
    for line_number, line in enumerate(file, 1):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        if isinstance(record, dict):
            for key, value in record.items():
                key_lower = str(key).lower()

                if any(word in key_lower for word in keywords):
                    counts[key] += 1
                    examples.setdefault(key, value)

        if line_number >= 1000:
            break

print("\nMatching fields:")
if not counts:
    print("No matching field names found.")
else:
    for key, count in counts.items():
        print(f"\nField: {key}")
        print(f"Occurrences: {count}")
        print(f"Example value: {str(examples[key])[:150]}")
