from kobo_api import extract_data_from_kobo
from config import KOBO_API_URL, KOBO_API_HEADERS
from db_utils import save_data_to_db

def run_pipeline():
    print("Starting data extraction process from KoboToolbox...")
    
    # Extracting data
    data = extract_data_from_kobo(KOBO_API_URL, KOBO_API_HEADERS)
    
    # Extracting records from the 'results' key
    if data and 'results' in data:
        records = data['results']
        print(f"Successfully extracted {len(records)} records.")
                
        # Saving records to the database
        save_data_to_db(records)
        print("Data saved to the database successfully.")
    else:
        print("No data extracted or failed to extract data.")

if __name__ == '__main__':
    run_pipeline()