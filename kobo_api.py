# kobo_api.py
import requests

def extract_data_from_kobo(api_url, headers):
    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while extracting data: {e}")
        return None
