import os
import requests
from dotenv import load_dotenv

load_dotenv()

TOLLGURU_API_KEY = os.getenv('TOLLGURU_API_KEY')
TOLLGURU_API_URL = os.getenv('TOLLGURU_API_URL')

def send_csv_to_tollguru(file_path):
    url = f"{TOLLGURU_API_URL}/gps-tracks-csv-upload?mapProvider=osrm&vehicleType=5AxlesTruck"
    headers = {'x-api-key': TOLLGURU_API_KEY, 'Content-Type': 'text/csv'}

    with open(file_path, 'rb') as file:
        response = requests.post(url, data=file, headers=headers)

    if response.status_code == 200:
        json_response = response.json()
        # Save the JSON response to a file with the same name as the CSV file
        json_file_path = os.path.splitext(file_path)[0] + '.json'
        with open(json_file_path, 'w') as json_file:
            json_file.write(json.dumps(json_response))
    else:
        print(f"Error sending CSV file {file_path}: {response.text}")


import os
import multiprocessing
from process1 import get_csv_files
from process2 import send_csv_to_tollguru

if __name__ == '__main__':
    csv_folder = '/output/process1'
    output_folder = '/output/process2'

    # Get a list of CSV files to process
    csv_files = get_csv_files(csv_folder)

    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Send each CSV file to the TollGuru API concurrently
    with multiprocessing.Pool() as pool:
        pool.map(send_csv_to_tollguru, csv_files)


