"""
Data Extraction Module

This module handles extracting data from different sources:
- Airport data from CSV file
- Live flight data from OpenSky Network API
"""

import pandas as pd
import requests
import time
import os

def extract_airports():
    """
    Extract airport data from CSV file
    
    Returns:
        pandas.DataFrame: Airport data with columns like name, city, country, coordinates
    """
    print("📄 Reading airport data from CSV...")
    
    try:
        # TODO: Read the airports.csv file using pandas
        # The file is located at: data/airports.csv
        # Hint: Use pd.read_csv()

        df = pd.read_csv("/home/barnabe/ETL-AirLife/data/airports.csv")
        
        # TODO: Print how many airports were loaded
        # Example: print(f"Loaded {len(df)} airports")
        
        # print("⚠️  Airport extraction not yet implemented")
        print(f"Loaded {len(df)} airports")
        return df
        
    except Exception as e:
        print(f"❌ Error reading airport data: {e}")
        return pd.DataFrame()

def extract_flights():
    """
    Extract current flight data from OpenSky Network API
    
    Returns:
        pandas.DataFrame: Flight data with current aircraft positions
    """
    print("🌐 Fetching live flight data from API...")
    
    # API endpoint for OpenSky Network
    url = "https://opensky-network.org/api/states/all"
    
    # Parameters to limit to a smaller area (Europe) to reduce data size
    params = {
        'lamin': 45,  # South boundary (latitude)
        'lomin': 5,   # West boundary (longitude) 
        'lamax': 50,  # North boundary (latitude)
        'lomax': 15   # East boundary (longitude)
    }
    
    try:
        print("Making API request... (this may take a few seconds)")
        
        # TODO: Make the API request using requests.get()
        # Hint: response = requests.get(url, params=params, timeout=10)

        response = requests.get(url, params=params, timeout=10)
        
        # TODO: Check if the response is successful
        # Hint: Check response.status_code == 200

        if response.status_code == 200:
            # Response is successful
            # Proceed to process the data
            pass
        else:
            print(f"⚠️ API returned status code: {response.status_code}")
            return pd.DataFrame()
        
        # TODO: Get the JSON data from the response
        # Hint: data = response.json()

        data = response.json()
        
        # TODO: Extract the 'states' data from the JSON
        # The API returns: {"time": 123456789, "states": [[aircraft_data], [aircraft_data], ...]}
        # Hint: states = data['states'] if data['states'] else []

        states = data['states'] if data['states'] else []
        
        # TODO: Convert to DataFrame
        # Hint: df = pd.DataFrame(states)

        df = pd.DataFrame(states)
        
        # TODO: Print how many flights were found
        # Example: print(f"Found {len(df)} active flights")

        print(f"Found {len(df)} active flights")

        return df
        
        # For now, return empty DataFrame
        # print("⚠️  Flight extraction not yet implemented")
        # return pd.DataFrame()
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error fetching flight data: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error processing flight data: {e}")
        return pd.DataFrame()

def test_api_connection():
    """
    Test function to check if the OpenSky API is accessible
    Students can use this to debug connection issues
    """
    print("🔍 Testing API connection...")
    
    try:
        response = requests.get(
            "https://opensky-network.org/api/states/all",
            params={'lamin': 45, 'lomin': 5, 'lamax': 46, 'lomax': 6},
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            flight_count = len(data['states']) if data['states'] else 0
            print(f"✅ API connection successful! Found {flight_count} flights in test area")
            return True
        else:
            print(f"⚠️ API returned status code: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ API connection failed: {e}")
        return False

if __name__ == "__main__":
    """Test the extraction functions"""
    print("Testing extraction functions...\n")
    
    # Test airport extraction
    airports = extract_airports()
    print(f"Airport extraction returned DataFrame with shape: {airports.shape}")
    
    # Test API connection first
    if test_api_connection():
        # Test flight extraction
        flights = extract_flights()
        print(f"Flight extraction returned DataFrame with shape: {flights.shape}")
    else:
        print("Skipping flight extraction due to API issues")

