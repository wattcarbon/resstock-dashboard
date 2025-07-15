#!/usr/bin/env python3
"""
Script to convert ResStock parquet file to SQLite database with county-level summaries
This creates aggregated data for visualizations without storing raw building data.
"""

import pandas as pd
import sqlite3
import os
import json
from pathlib import Path
from tqdm import tqdm

def convert_parquet_to_sqlite(parquet_file='baseline.parquet', db_file='resstock.db'):
    """
    Convert parquet file to SQLite database with county-level summaries
    
    Args:
        parquet_file (str): Path to the parquet file
        db_file (str): Path to the output SQLite database
    """
    print(f"🔄 Converting {parquet_file} to SQLite database...")
    
    # Check if parquet file exists
    if not os.path.exists(parquet_file):
        print(f"❌ Error: {parquet_file} not found!")
        return False
    
    try:
        # Load parquet file
        print("📖 Loading parquet file...")
        df = pd.read_parquet(parquet_file)
        
        # Reset index to make bldg_id a regular column
        df = df.reset_index()
        
        # Replace periods with underscores in column names for SQLite compatibility
        print("🔄 Cleaning column names...")
        df.columns = df.columns.str.replace('.', '_')
        
        print(f"✅ Loaded {len(df):,} rows with {len(df.columns)} columns")
        
        # Create comprehensive county summary
        print("📊 Creating county-level summaries...")
        county_summary = create_county_summary(df)
        
        # Create SQLite connection
        print("🗄️ Creating SQLite database...")
        conn = sqlite3.connect(db_file)
        
        # Write county summary to SQLite
        print("💾 Writing county summary to SQLite...")
        county_summary.to_sql('county_summary', conn, if_exists='replace', index=False)
        
        # Optionally save raw building data (commented out for performance)
        # Uncomment the following lines if you need access to individual building records
        # print("💾 Writing raw building data to SQLite...")
        # df.to_sql('buildings', conn, if_exists='replace', index=False)
        
        # Create indexes for better query performance
        print("⚡ Creating indexes...")
        cursor = conn.cursor()
        
        # Primary index on county ID for fast lookups
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_county_id ON county_summary (in_county)")
        
        # Index on county name and state for fast filtering
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_county_state ON county_summary (in_county_name, in_state)")
        
        # Index on state for state-level filtering
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_state ON county_summary (in_state)")
        
        # Raw data indexes (commented out - uncomment if saving raw data)
        # cursor.execute("CREATE INDEX IF NOT EXISTS idx_buildings_county_id ON buildings (in_county)")
        # cursor.execute("CREATE INDEX IF NOT EXISTS idx_buildings_county_state ON buildings (in_county_name, in_state)")
        # cursor.execute("CREATE INDEX IF NOT EXISTS idx_buildings_building_type ON buildings (in_geometry_building_type_recs)")
        # cursor.execute("CREATE INDEX IF NOT EXISTS idx_buildings_vintage ON buildings (in_vintage)")
        # cursor.execute("CREATE INDEX IF NOT EXISTS idx_buildings_heating_fuel ON buildings (in_heating_fuel)")
        # cursor.execute("CREATE INDEX IF NOT EXISTS idx_buildings_floor_area ON buildings (in_geometry_floor_area)")
        
        conn.commit()
        
        # Get some basic stats
        cursor.execute("SELECT COUNT(*) FROM county_summary")
        total_counties = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT in_state) FROM county_summary")
        total_states = cursor.fetchone()[0]
        
        cursor.execute("SELECT SUM(building_count) FROM county_summary")
        total_buildings = cursor.fetchone()[0]
        
        # Get sample counties
        cursor.execute("SELECT in_county_name, in_state, building_count FROM county_summary ORDER BY building_count DESC LIMIT 5")
        sample_counties = cursor.fetchall()
        
        conn.close()
        
        print(f"✅ Successfully converted to SQLite!")
        print(f"📊 Database stats:")
        print(f"   - Total counties: {total_counties:,}")
        print(f"   - Total states: {total_states}")
        print(f"   - Total buildings: {total_buildings:,}")
        print(f"   - Top counties by building count:")
        for county, state, count in sample_counties:
            print(f"     • {county}, {state}: {count:,} buildings")
        print(f"   - Database file: {db_file}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during conversion: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def create_county_summary(df):
    """
    Create comprehensive county-level summary with all data needed for visualizations
    """
    print("📋 Creating comprehensive county summary...")
    
    # Group by county and create comprehensive summary
    county_groups = df.groupby(['in_county', 'in_county_name', 'in_state'])
    
    summary_data = []
    
    # Create progress bar with dynamic county names
    pbar = tqdm(county_groups, desc="Processing counties", unit="county")
    
    for (county_id, county_name, state), group in pbar:
        # Update progress bar description with current county
        pbar.set_description(f"Processing {county_name}, {state}")
        pbar.set_postfix(buildings=len(group))
        
        # Basic counts and averages
        building_count = len(group)
        weighted_count = group['weight'].sum() if 'weight' in group.columns else building_count
        
        # Building characteristics
        avg_floor_area = None
        if 'in_geometry_floor_area' in group.columns:
            try:
                avg_floor_area = pd.to_numeric(group['in_geometry_floor_area'], errors='coerce').mean()
            except:
                avg_floor_area = None
                
        avg_vintage = None
        if 'in_vintage' in group.columns:
            try:
                avg_vintage = pd.to_numeric(group['in_vintage'], errors='coerce').mean()
            except:
                avg_vintage = None
        
        # Energy metrics
        avg_electricity_kwh = None
        if 'out_electricity_total_energy_consumption' in group.columns:
            try:
                avg_electricity_kwh = pd.to_numeric(group['out_electricity_total_energy_consumption'], errors='coerce').mean()
            except:
                avg_electricity_kwh = None
                
        avg_electric_bill = None
        if 'out_bills_electricity_usd' in group.columns:
            try:
                avg_electric_bill = pd.to_numeric(group['out_bills_electricity_usd'], errors='coerce').mean()
            except:
                avg_electric_bill = None
                
        avg_energy_burden = None
        if 'out_energy_burden_percentage' in group.columns:
            try:
                avg_energy_burden = pd.to_numeric(group['out_energy_burden_percentage'], errors='coerce').mean()
            except:
                avg_energy_burden = None
        
        # Building type distribution (as JSON for easy access)
        building_type_dist = {}
        if 'in_geometry_building_type_recs' in group.columns:
            building_type_counts = group['in_geometry_building_type_recs'].value_counts()
            building_type_dist = {
                'types': building_type_counts.index.tolist(),
                'counts': building_type_counts.values.tolist(),
                'percentages': (building_type_counts / len(group) * 100).tolist()
            }
        
        # Heating fuel distribution
        heating_fuel_dist = {}
        if 'in_heating_fuel' in group.columns:
            heating_fuel_counts = group['in_heating_fuel'].value_counts()
            heating_fuel_dist = {
                'fuels': heating_fuel_counts.index.tolist(),
                'counts': heating_fuel_counts.values.tolist(),
                'percentages': (heating_fuel_counts / len(group) * 100).tolist()
            }
        
        # Water heater fuel distribution
        water_heater_fuel_dist = {}
        if 'in_water_heater_fuel' in group.columns:
            water_heater_fuel_counts = group['in_water_heater_fuel'].value_counts()
            water_heater_fuel_dist = {
                'fuels': water_heater_fuel_counts.index.tolist(),
                'counts': water_heater_fuel_counts.values.tolist(),
                'percentages': (water_heater_fuel_counts / len(group) * 100).tolist()
            }
        
        # Vintage distribution (decade bins)
        vintage_dist = {}
        if 'in_vintage' in group.columns:
            try:
                # Create decade bins
                group_copy = group.copy()
                vintage_numeric = pd.to_numeric(group_copy['in_vintage'], errors='coerce')
                group_copy['decade'] = (vintage_numeric // 10) * 10
                decade_counts = group_copy['decade'].value_counts().sort_index()
                vintage_dist = {
                    'decades': decade_counts.index.tolist(),
                    'counts': decade_counts.values.tolist(),
                    'percentages': (decade_counts / len(group) * 100).tolist()
                }
            except:
                vintage_dist = {}
        
        # Floor area distribution (size bins)
        floor_area_dist = {}
        if 'in_geometry_floor_area' in group.columns:
            try:
                # Create size bins
                group_copy = group.copy()
                floor_area_numeric = pd.to_numeric(group_copy['in_geometry_floor_area'], errors='coerce')
                group_copy['size_bin'] = pd.cut(
                    floor_area_numeric, 
                    bins=[0, 1000, 2000, 3000, 5000, float('inf')],
                    labels=['<1k', '1k-2k', '2k-3k', '3k-5k', '>5k']
                )
                size_counts = group_copy['size_bin'].value_counts()
                floor_area_dist = {
                    'bins': size_counts.index.tolist(),
                    'counts': size_counts.values.tolist(),
                    'percentages': (size_counts / len(group) * 100).tolist()
                }
            except:
                floor_area_dist = {}
        
        # Energy consumption distribution
        energy_dist = {}
        if 'out_electricity_total_energy_consumption' in group.columns:
            try:
                # Create energy consumption bins
                group_copy = group.copy()
                energy_numeric = pd.to_numeric(group_copy['out_electricity_total_energy_consumption'], errors='coerce')
                group_copy['energy_bin'] = pd.cut(
                    energy_numeric,
                    bins=[0, 5000, 10000, 15000, 20000, float('inf')],
                    labels=['<5k', '5k-10k', '10k-15k', '15k-20k', '>20k']
                )
                energy_counts = group_copy['energy_bin'].value_counts()
                energy_dist = {
                    'bins': energy_counts.index.tolist(),
                    'counts': energy_counts.values.tolist(),
                    'percentages': (energy_counts / len(group) * 100).tolist()
                }
            except:
                energy_dist = {}
        
        # Create summary row
        summary_row = {
            'in_county': county_id,
            'in_county_name': county_name,
            'in_state': state,
            'building_count': building_count,
            'weighted_count': weighted_count,
            'avg_floor_area': avg_floor_area,
            'avg_vintage': avg_vintage,
            'avg_electricity_kwh': avg_electricity_kwh,
            'avg_electric_bill': avg_electric_bill,
            'avg_energy_burden': avg_energy_burden,
            'building_type_dist': json.dumps(building_type_dist),
            'heating_fuel_dist': json.dumps(heating_fuel_dist),
            'water_heater_fuel_dist': json.dumps(water_heater_fuel_dist),
            'vintage_dist': json.dumps(vintage_dist),
            'floor_area_dist': json.dumps(floor_area_dist),
            'energy_dist': json.dumps(energy_dist)
        }
        
        summary_data.append(summary_row)
    
    # Create DataFrame
    summary_df = pd.DataFrame(summary_data)
    
    print(f"✅ Created summary for {len(summary_df)} counties")
    return summary_df

if __name__ == "__main__":
    # Convert parquet to SQLite with county summaries
    success = convert_parquet_to_sqlite()
    
    if success:
        print("\n🎉 Conversion complete! The SQLite database contains county-level summaries for the dashboard.")
        print("📊 The county_summary table includes:")
        print("   • Building counts and demographics")
        print("   • Energy consumption and cost metrics")
        print("   • Building type distributions (JSON)")
        print("   • Fuel usage distributions (JSON)")
        print("   • Vintage and floor area distributions (JSON)")
        print("   • Energy consumption distributions (JSON)")
    else:
        print("\n❌ Conversion failed. Please check the error messages above.") 