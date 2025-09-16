#!/usr/bin/env python3
"""
AirLife ETL Pipeline - Full Version

Run the complete ETL pipeline:
1. Extract airport data from CSV and flight data from API
2. Clean and transform the data
3. Load the data into PostgreSQL database
4. Verify the loaded data
"""

from src.extract_data import extract_airports, extract_flights
from src.transform_data import clean_airports, clean_flights  # combine_data optional if needed
from src.load_data import load_to_database, verify_data

def main():
    """Run the complete ETL pipeline"""
    print("🛫 Starting AirLife ETL Pipeline...")
    print("=" * 50)
    
    try:
        # Step 1: Extract data
        print("\n=== EXTRACTION ===")
        print("📥 Extracting data from sources...")
        airports = extract_airports()
        flights = extract_flights()
        
        # Step 2: Transform data
        print("\n=== TRANSFORMATION ===")
        print("🔄 Cleaning and transforming data...")
        clean_airports_data = clean_airports(airports)
        clean_flights_data = clean_flights(flights)
        # If you have combine_data implemented:
        # final_airports, final_flights = combine_data(clean_airports_data, clean_flights_data)
        final_airports, final_flights = clean_airports_data, clean_flights_data
        
        # Step 3: Load data
        print("\n=== LOADING ===")
        print("💾 Loading data to database...")
        load_to_database(final_airports, final_flights)
        
        # Step 4: Verify data
        print("\n=== VERIFICATION ===")
        print("🔍 Verifying data was loaded correctly...")
        verify_data()
        
        print("\n🎉 ETL Pipeline completed successfully!")
        print("=" * 50)
    
    except Exception as e:
        print(f"❌ ETL Pipeline failed: {e}")
        print("💡 Check logs and data sources before retrying.")

if __name__ == "__main__":
    main()
