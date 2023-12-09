import os
import argparse
import json

def process_json_files(json_folder_path):
    # Get a list of JSON files to process
    json_files = [os.path.join(json_folder_path, f) for f in os.listdir(json_folder_path) if f.endswith('.json')]

    # Process each JSON file and concatenate the results
    output = 'unit,trip_id,toll_loc_id_start,toll_loc_id_end,toll_loc_name_start,toll_loc_name_end,toll_system_type,entry_time,exit_time,tag_cost,cash_cost,license_plate_cost\n'
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
                filename = os.path.basename(json_file)
                unit = os.path.splitext(filename)[0]
                trip_id = os.path.splitext(filename)[0]
                unit = unit.split('_')[0]
                print(trip_id)
                output += f"{unit},{trip_id}"

                output += f"{data.get('toll_loc_id_start', '')},{data.get('toll_loc_id_end', '')},"
                output += f"{data.get('toll_loc_name_start', '')},{data.get('toll_loc_name_end', '')},"
                output += f"{data.get('toll_system_type', '')},{data.get('entry_time', '')},"
                output += f"{data.get('exit_time', '')},{data.get('tag_cost', '')},"
                output += f"{data.get('cash_cost', '')},{data.get('license_plate_cost', '')}\n"
        except Exception as e:
            print(f"Error processing {json_file}: {e}")
    return output
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--to_process', type=str, required=True, help='Path to the JSON folder')
    parser.add_argument('--output_dir', type=str, required=True, help='Path to the output file')
    args = parser.parse_args()
    output = process_json_files(args.to_process)
    with open(os.path.join(args.output_dir, 'transformed_data.csv'), 'w') as f:
        f.write(output)
