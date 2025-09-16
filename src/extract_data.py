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
        pandas.DataFrame: Airport data with standardized columns
    """
    print("📄 Reading airport data from CSV...")

    try:
        # Load the CSV
        df = pd.read_csv("/home/jmaubian/SDD/fsd312/ETL-AirLife/data/airports.csv")

        # If the file doesn’t have headers, assign standard ones
        expected_columns = [
            "id", "name", "city", "country", "iata", "icao",
            "latitude", "longitude", "altitude", "timezone",
            "dst", "tz_database_time_zone", "type", "source"
        ]

        if len(df.columns) == len(expected_columns):
            df.columns = expected_columns

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

    url = "https://opensky-network.org/api/states/all"
    params = {
        'lamin': 45,  # South boundary (latitude)
        'lomin': 5,   # West boundary (longitude) 
        'lamax': 50,  # North boundary (latitude)
        'lomax': 15   # East boundary (longitude)
    }

    try:
        print("Making API request... (this may take a few seconds)")
        response = requests.get(url, params=params, timeout=10)

        if response.status_code != 200:
            print(f"⚠️ API returned status code: {response.status_code}")
            return pd.DataFrame()

        data = response.json()
        states = data.get("states", []) or []   # safe lookup

        columns = [
            "icao24", "callsign", "origin_country", "time_position", "last_contact",
            "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
            "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
            "spi", "position_source"
        ]

        # handle case where columns mismatch
        df = pd.DataFrame(states, columns=columns[:len(states[0])] if states else columns)

        print(f"Found {len(df)} active flights")
        
        return df

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
