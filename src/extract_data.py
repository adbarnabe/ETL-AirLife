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
        # Lire le fichier CSV
        df = pd.read_csv("data/airports.csv")
        
        # Afficher combien d'aéroports ont été chargés
        print(f"✅ Loaded {len(df)} airports")
        
        return df
        
    except FileNotFoundError:
        print("❌ Error: 'data/airports.csv' not found")
        return pd.DataFrame()
        
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
    
    # API endpoint
    url = "https://opensky-network.org/api/states/all"
    
    # Parameters for Europe to reduce data size
    params = {
        'lamin': 45,  # South latitude
        'lomin': 5,   # West longitude
        'lamax': 50,  # North latitude
        'lomax': 15   # East longitude
    }
    
    try:
        print("Making API request... (this may take a few seconds)")
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # lève une erreur si code HTTP != 200
        
        data = response.json()
        states = data.get('states', [])
        
        if not states:
            print("⚠️ No flights found")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(states, columns=[
            "icao24", "callsign", "origin_country", "time_position", "last_contact",
            "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
            "heading", "vertical_rate", "sensors", "geo_altitude", "squawk",
            "spi", "position_source"
        ])
        
        print(f"✅ Found {len(df)} active flights")
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
