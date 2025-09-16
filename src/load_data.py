"""
Data Loading Module

This module handles loading cleaned data into PostgreSQL database:
- Load airport data to airports table
- Load flight data to flights table  
- Verify data was loaded correctly
"""

import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2

# Database connection configuration
# TODO: Update these values with your actual database credentials
DATABASE_CONFIG = {
    'username': 'barnabe',
    'password': '', 
    'host': 'localhost',
    'port': '5432',
    'database': 'airlife_db'
}

def get_connection_string():
    """Build PostgreSQL connection string"""
    return f"postgresql://{DATABASE_CONFIG['username']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"

def load_to_database(airports_df, flights_df):
    """
    Load cleaned data into PostgreSQL database
    
    Args:
        airports_df (pandas.DataFrame): Cleaned airport data
        flights_df (pandas.DataFrame): Cleaned flight data
    """
    print("💾 Loading data to PostgreSQL database...")
    
    # TODO: Create connection string using the function above
    connection_string = get_connection_string()
    
    try:
        # TODO: Create SQLAlchemy engine
        # Hint: engine = create_engine(connection_string)
        engine = create_engine(connection_string)
    
        
        # TODO: Load airports data
        # Use pandas to_sql method to insert data
        # Hint: airports_df.to_sql('airports', engine, if_exists='replace', index=False)
        # 
        # Parameters explanation:
        # - 'airports': table name in database
        # - engine: database connection
        # - if_exists='replace': replace table if it exists (use 'append' to add to existing data)
        # - index=False: don't include pandas row index as a column

        airports_df.to_sql('airports', engine, if_exists='replace', index=False)
        
        # TODO: Load flights data (only if not empty)
        # Check if flights_df is not empty before loading
        # Hint: if not flights_df.empty:
        #           flights_df.to_sql('flights', engine, if_exists='replace', index=False)

        if not flights_df.empty:
            flights_df.to_sql('flights', engine, if_exists='replace', index=False)
        
        # TODO: Print loading statistics
        print(f"✅ Loaded {len(airports_df)} airports to database")
        if not flights_df.empty:
            print(f"✅ Loaded {len(flights_df)} flights to database")
        else:
            print("ℹ️  No flight data to load")
        
    except Exception as e:
        print(f"❌ Error loading data to database: {e}")
        print("💡 Make sure:")
        print("   - PostgreSQL is running")
        print("   - Database 'airlife_db' exists") 
        print("   - Username and password are correct")
        print("   - Tables are created (run database_setup.sql)")

def verify_data():
    """
    Verify that data was loaded correctly by running some basic queries
    """
    print("🔍 Verifying data was loaded correctly...")
    
    connection_string = get_connection_string()
    
    try:
        # TODO: Create SQLAlchemy engine
        # engine = create_engine(connection_string)
        engine = create_engine(connection_string)
        
        # TODO: Count airports in database
        # Hint: airports_count = pd.read_sql("SELECT COUNT(*) as count FROM airports", engine)
        # print(f"📊 Airports in database: {airports_count.iloc[0]['count']}")
        airports_count = pd.read_sql("SELECT COUNT(*) as count FROM airports", engine)
        print(f"📊 Airports in database: {airports_count.iloc[0]['count']}")
        
        # TODO: Count flights in database  
        # Hint: flights_count = pd.read_sql("SELECT COUNT(*) as count FROM flights", engine)
        # print(f"📊 Flights in database: {flights_count.iloc[0]['count']}")
        flights_count = pd.read_sql("SELECT COUNT(*) as count FROM flights", engine)
        print(f"📊 Flights in database: {flights_count.iloc[0]['count']}")
        
        # TODO: Show sample airport data
        # Hint: sample_airports = pd.read_sql("SELECT name, city, country FROM airports LIMIT 3", engine)
        # print("\n📋 Sample airports:")
        # print(sample_airports.to_string(index=False))

        sample_airports = pd.read_sql("SELECT name, city, country FROM airports LIMIT 3", engine)
        print("\n📋 Sample airports:")
        print(sample_airports.to_string(index=False))
        
        # TODO: Show sample flight data (if any exists)
        # Hint: Check if flights table has data first
        # sample_flights = pd.read_sql("SELECT callsign, origin_country, altitude FROM flights LIMIT 3", engine)
        # if not sample_flights.empty:
        #     print("\n✈️  Sample flights:")
        #     print(sample_flights.to_string(index=False))

        sample_flights = pd.read_sql("SELECT callsign, origin_country, altitude FROM flights LIMIT 3", engine)
        if not sample_flights.empty:
            print("\n✈️  Sample flights:")
            print(sample_flights.to_string(index=False))
        
    except Exception as e:
        print(f"❌ Error verifying data: {e}")

def run_sample_queries():
    """
    Run some interesting queries on the loaded data
    Students can use this to explore their data
    """
    print("📈 Running sample analysis queries...")
    
    connection_string = get_connection_string()
    
    try:
        engine = create_engine(connection_string)
        
        # Query 1: Airports by country
        print("\n🌍 Top 5 countries by number of airports:")
        country_query = """
        SELECT country, COUNT(*) as airport_count 
        FROM airports 
        WHERE country IS NOT NULL 
        GROUP BY country 
        ORDER BY airport_count DESC 
        LIMIT 5
        """
        country_results = pd.read_sql(country_query, engine)
        print(country_results.to_string(index=False))
        
        # Query 2: Flight altitude analysis (if flight data exists)
        flight_check = pd.read_sql("SELECT COUNT(*) as count FROM flights", engine)
        if flight_check.iloc[0]['count'] > 0:
            print("\n✈️  Flight altitude statistics:")
            altitude_query = """
            SELECT 
                COUNT(*) as total_flights,
                ROUND(AVG(altitude)) as avg_altitude_ft,
                ROUND(MIN(altitude)) as min_altitude_ft,
                ROUND(MAX(altitude)) as max_altitude_ft
            FROM flights 
            WHERE altitude IS NOT NULL
            """
            altitude_results = pd.read_sql(altitude_query, engine)
            print(altitude_results.to_string(index=False))
        
    except Exception as e:
        print(f"❌ Error running sample queries: {e}")

def test_database_connection():
    """
    Test database connection without loading data
    Students can use this to debug connection issues
    """
    print("🔌 Testing database connection...")
    
    connection_string = get_connection_string()
    
    try:
        engine = create_engine(connection_string)
        
        # Try a simple query
        result = pd.read_sql("SELECT 1 as test", engine)
        
        if result.iloc[0]['test'] == 1:
            print("✅ Database connection successful!")
            
            # Check if our tables exist
            tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('airports', 'flights')
            ORDER BY table_name
            """
            tables = pd.read_sql(tables_query, engine)
            
            if len(tables) == 2:
                print("✅ Required tables (airports, flights) exist")
            else:
                print(f"⚠️  Found {len(tables)} tables, expected 2")
                print("💡 Run database_setup.sql to create tables")
            
            return True
        else:
            print("❌ Database connection test failed")
            return False
            
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("💡 Check your connection settings in DATABASE_CONFIG")
        return False

if __name__ == "__main__":
    """Test the loading functions"""
    print("Testing database loading functions...\n")
    
    # Test database connection first
    if test_database_connection():
        print("\nDatabase connection OK. Ready for data loading!")
        
        # Create some sample data for testing
        sample_airports = pd.DataFrame({
            'name': ['Test Airport'],
            'city': ['Test City'], 
            'country': ['Test Country'],
            'iata_code': ['TST'],
            'latitude': [48.8566],
            'longitude': [2.3522],
            'altitude': [100]
        })
        
        sample_flights = pd.DataFrame()  # Empty for testing
        
        # Test loading (won't work until students implement it)
        load_to_database(sample_airports, sample_flights)
    else:
        print("Fix database connection before testing loading functions")
