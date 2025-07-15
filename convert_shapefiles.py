#!/usr/bin/env python3
"""
Script to convert county shapefiles to GeoJSON format
and prepare data for interactive mapping in Streamlit
"""

import pdb
import geopandas as gpd
import pandas as pd
import json
import os

def convert_shapefiles_to_geojson():
    """
    Convert county shapefiles to GeoJSON format
    """
    print("🔄 Converting county shapefiles to GeoJSON...")
    
    # Check if shapefile exists
    shapefile_path = "tl_2022_us_county.shp"
    if not os.path.exists(shapefile_path):
        print(f"❌ Error: {shapefile_path} not found!")
        return False
    
    try:
        # Read the shapefile
        print("📖 Reading shapefile...")
        gdf = gpd.read_file(shapefile_path)
        
        print(f"✅ Loaded {len(gdf):,} counties")
        print(f"📊 Columns: {list(gdf.columns)}")
        
        # Display sample data
        print("\n📋 Sample county data:")
        print(gdf.head()[['NAME', 'STATEFP', 'GEOID']])
        
        # Clean up column names and data
        print("\n🔄 Cleaning data...")
        
        # We need to get state names from FIPS codes
        # Create a state FIPS to name mapping
        state_fips_mapping = {
            '01': 'Alabama', '02': 'Alaska', '04': 'Arizona', '05': 'Arkansas', '06': 'California',
            '08': 'Colorado', '09': 'Connecticut', '10': 'Delaware', '11': 'District of Columbia',
            '12': 'Florida', '13': 'Georgia', '15': 'Hawaii', '16': 'Idaho', '17': 'Illinois',
            '18': 'Indiana', '19': 'Iowa', '20': 'Kansas', '21': 'Kentucky', '22': 'Louisiana',
            '23': 'Maine', '24': 'Maryland', '25': 'Massachusetts', '26': 'Michigan', '27': 'Minnesota',
            '28': 'Mississippi', '29': 'Missouri', '30': 'Montana', '31': 'Nebraska', '32': 'Nevada',
            '33': 'New Hampshire', '34': 'New Jersey', '35': 'New Mexico', '36': 'New York',
            '37': 'North Carolina', '38': 'North Dakota', '39': 'Ohio', '40': 'Oklahoma',
            '41': 'Oregon', '42': 'Pennsylvania', '44': 'Rhode Island', '45': 'South Carolina',
            '46': 'South Dakota', '47': 'Tennessee', '48': 'Texas', '49': 'Utah', '50': 'Vermont',
            '51': 'Virginia', '53': 'Washington', '54': 'West Virginia', '55': 'Wisconsin', '56': 'Wyoming'
        }
        
        # Rename columns for consistency
        column_mapping = {
            'NAME': 'county_name',
            'STATEFP': 'state_fips',
            'COUNTYFP': 'county_fips',
            'GEOID': 'geoid',
            'ALAND': 'land_area',
            'AWATER': 'water_area'
        }
        
        gdf = gdf.rename(columns=column_mapping)
        
        # Add state names from FIPS codes
        gdf['state_name'] = gdf['state_fips'].map(state_fips_mapping)
        
        # Keep only essential columns
        essential_columns = ['county_name', 'state_name', 'state_fips', 'county_fips', 'geoid', 'geometry']
        gdf = gdf[essential_columns]
        
        # Create a combined county identifier for matching with ResStock data
        gdf['county_state'] = gdf['county_name'] + ', ' + gdf['state_name']
        
        # Add county_id for ResStock matching
        gdf['county_id'] = 'G' + gdf['state_fips'].str.zfill(2) + '0' + gdf['county_fips'].str.zfill(3) + '0'
        
        # '0' +   to GeoJSON
        print("💾 Converting to GeoJSON...")
        geojson_data = json.loads(gdf.to_json())
        
        # Save full GeoJSON
        with open('counties.geojson', 'w') as f:
            json.dump(geojson_data, f)
        
        print(f"✅ Saved GeoJSON with {len(gdf):,} counties")
        print(f"📁 File: counties.geojson")
        
        # Create state-specific GeoJSON files for faster filtering
        print("\n🗂️ Creating state-specific GeoJSON files...")
        states_dir = "states_geojson"
        os.makedirs(states_dir, exist_ok=True)
        
        for state in gdf['state_name'].dropna().unique():
            state_data = gdf[gdf['state_name'] == state]
            state_geojson = json.loads(state_data.to_json())
            
            # Use state FIPS code for filename
            state_fips = state_data['state_fips'].iloc[0]
            filename = f"{states_dir}/{state_fips}.geojson"
            
            with open(filename, 'w') as f:
                json.dump(state_geojson, f)
        
        print(f"✅ Created {len(gdf['state_name'].unique())} state-specific GeoJSON files in {states_dir}/")
        
        # Create state lookup table with 2-digit state abbreviations
        state_abbrev_mapping = {
            'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
            'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'District of Columbia': 'DC',
            'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL',
            'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA',
            'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN',
            'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
            'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
            'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
            'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
            'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
            'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'
        }
        
        state_lookup = gdf[['state_name', 'state_fips']].drop_duplicates().sort_values('state_name')
        state_lookup['state_abbrev'] = state_lookup['state_name'].map(state_abbrev_mapping)
        state_lookup.to_csv('state_lookup.csv', index=False)
        print("✅ State lookup table saved as state_lookup.csv")
        
        # Create a lookup table for county matching
        print("\n📋 Creating county lookup table...")
        lookup_df = gdf[['county_name', 'state_name', 'county_state', 'geoid']].copy()
        lookup_df.to_csv('county_lookup.csv', index=False)
        
        print("✅ County lookup table saved as county_lookup.csv")
        
        # Show some statistics
        print(f"\n📊 Statistics:")
        print(f"   - Total counties: {len(gdf):,}")
        print(f"   - Unique states: {gdf['state_name'].nunique()}")
        print(f"   - Sample counties: {list(gdf['county_state'].head())}")
        
        return True
        
    except Exception as e:
        pdb.post_mortem()
        print(f"❌ Error during conversion: {str(e)}")
        return False

def create_county_matching_data():
    """
    Create a mapping between ResStock counties and shapefile counties
    """
    print("\n🔗 Creating county matching data...")
    
    try:
        # Load ResStock county data
        import sqlite3
        conn = sqlite3.connect('resstock.db')
        
        # Get unique counties from ResStock data
        resstock_counties = pd.read_sql_query("""
            SELECT DISTINCT in_county_name, in_state 
            FROM buildings 
            ORDER BY in_county_name, in_state
        """, conn)
        
        conn.close()
        
        # Load shapefile counties
        gdf = gpd.read_file("tl_2022_us_county.shp")
        
        # Create state FIPS to name mapping
        state_fips_mapping = {
            '01': 'Alabama', '02': 'Alaska', '04': 'Arizona', '05': 'Arkansas', '06': 'California',
            '08': 'Colorado', '09': 'Connecticut', '10': 'Delaware', '11': 'District of Columbia',
            '12': 'Florida', '13': 'Georgia', '15': 'Hawaii', '16': 'Idaho', '17': 'Illinois',
            '18': 'Indiana', '19': 'Iowa', '20': 'Kansas', '21': 'Kentucky', '22': 'Louisiana',
            '23': 'Maine', '24': 'Maryland', '25': 'Massachusetts', '26': 'Michigan', '27': 'Minnesota',
            '28': 'Mississippi', '29': 'Missouri', '30': 'Montana', '31': 'Nebraska', '32': 'Nevada',
            '33': 'New Hampshire', '34': 'New Jersey', '35': 'New Mexico', '36': 'New York',
            '37': 'North Carolina', '38': 'North Dakota', '39': 'Ohio', '40': 'Oklahoma',
            '41': 'Oregon', '42': 'Pennsylvania', '44': 'Rhode Island', '45': 'South Carolina',
            '46': 'South Dakota', '47': 'Tennessee', '48': 'Texas', '49': 'Utah', '50': 'Vermont',
            '51': 'Virginia', '53': 'Washington', '54': 'West Virginia', '55': 'Wisconsin', '56': 'Wyoming'
        }
        
        gdf['state_name'] = gdf['STATEFP'].map(state_fips_mapping)
        gdf['county_state'] = gdf['NAME'] + ', ' + gdf['state_name']
        
        # Create matching table
        resstock_counties['county_state'] = resstock_counties['in_county_name'] + ', ' + resstock_counties['in_state']
        
        # Find matches
        matches = resstock_counties.merge(
            gdf[['county_state', 'GEOID', 'geometry']], 
            on='county_state', 
            how='left'
        )
        
        # Save matches
        matches.to_csv('resstock_county_matches.csv', index=False)
        
        matched_count = matches['GEOID'].notna().sum()
        total_count = len(matches)
        
        print(f"✅ County matching complete!")
        print(f"📊 Matched {matched_count:,} out of {total_count:,} counties ({matched_count/total_count*100:.1f}%)")
        print(f"📁 File: resstock_county_matches.csv")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during county matching: {str(e)}")
        return False

if __name__ == "__main__":
    # Convert shapefiles to GeoJSON
    success = convert_shapefiles_to_geojson()
    
    if success:
        # Create county matching data
        create_county_matching_data()
        print("\n🎉 Shapefile conversion complete! Ready for interactive mapping.")
    else:
        print("\n❌ Conversion failed. Please check the error messages above.") 