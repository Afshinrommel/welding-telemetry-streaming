import json
import os

def extract_and_shift_simulation_data(input_path, output_filename, duration_window=5.0, new_start_time=100.0, new_end_time=110.0):
    """
    Scans a JSONL simulation file to find the max time, extracts the last N seconds,
    linearly shifts/maps their timestamps to a new time window (e.g., 100s to 110s) 
    to prevent duplicate timestamp conflicts in Kafka and MongoDB, and saves them 
    explicitly into the target directory.
    """
    if not os.path.exists(input_path):
        print(f"Error: Input file '{input_path}' not found.")
        return

    # Explicit target directory path as requested
    target_directory = r"C:\Users\Dell\Documents\Data_Engineering_Project\result_data_welding"
    
    # Ensure the directory exists
    os.makedirs(target_directory, exist_ok=True)
    
    # Combine directory with the output filename
    output_path = os.path.join(target_directory, output_filename)

    print(f"Analyzing {input_path} to determine time range...")
    max_time = -1.0
    
    # Pass 1: Find the maximum time step efficiently line-by-line
    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f:
            line_str = line.strip()
            if not line_str:
                continue
            try:
                record = json.loads(line_str)
                t = float(record.get("time", 0.0))
                if t > max_time:
                    max_time = t
            except json.JSONDecodeError:
                continue

    if max_time < 0:
        print(f"Error: No valid time steps found in {input_path}.")
        return

    start_target_time = max_time - duration_window
    print(f"Original max time: {max_time}s. Extracting window from {start_target_time:.2f}s to {max_time}s...")
    print(f"Shifting timestamps to new range: {new_start_time}s to {new_end_time}s...")

    extracted_count = 0
    original_duration = max_time - start_target_time
    new_duration = new_end_time - new_start_time

    # Pass 2: Extract, shift timestamps, and write records to the explicit output path
    with open(input_path, 'r', encoding='utf-8') as fin, open(output_path, 'w', encoding='utf-8') as fout:
        for line in fin:
            line_str = line.strip()
            if not line_str:
                continue
            try:
                record = json.loads(line_str)
                t = float(record.get("time", 0.0))
                
                # Check if the record falls within the target recent time window
                if t >= start_target_time:
                    # Linearly map old timestamp to the new time range (e.g., 100 to 110)
                    if original_duration > 0:
                        normalized_progress = (t - start_target_time) / original_duration
                        new_t = new_start_time + (normalized_progress * new_duration)
                    else:
                        new_t = new_start_time
                    
                    # Update the time field in the record
                    record["time"] = round(new_t, 4)
                    
                    # Write the updated record as a JSONL line
                    fout.write(json.dumps(record) + "\n")
                    extracted_count += 1
            except json.JSONDecodeError:
                continue

    print(f"Successfully created '{output_path}' with {extracted_count} shifted records (Range: {new_start_time}s - {new_end_time}s).\n")

if __name__ == "__main__":
    # Process mechanical simulation results and save in the exact specified directory
    extract_and_shift_simulation_data(
        input_path=r"C:\Users\Dell\Documents\Data_Engineering_Project\result_data_welding\mechanical_results.jsonl",
        output_filename="mechanical_results_100_110.jsonl",
        duration_window=5.0,
        new_start_time=100.0,
        new_end_time=110.0
    )
    
    # Process thermal series results and save in the exact specified directory
    extract_and_shift_simulation_data(
        input_path=r"C:\Users\Dell\Documents\Data_Engineering_Project\result_data_welding\thermal_series.jsonl",
        output_filename="thermal_series_100_110.jsonl",
        duration_window=5.0,
        new_start_time=100.0,
        new_end_time=110.0
    )