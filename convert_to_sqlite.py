#!/usr/bin/env python3
"""
Script to convert ResStock parquet file to SQLite database with county-level summaries
This creates aggregated data for visualizations without storing raw building data.
"""

import pandas as pd
import sqlite3
import os
from pathlib import Path
from tqdm import tqdm

# Distribution data (compact format for visualizations)
# Configuration: Add column names here to automatically include them in distributions
distribution_columns = [
    'in_geometry_building_type_recs',
    'in_heating_fuel', 
    'in_water_heater_fuel',
    'in_vintage',  # Add vintage distribution
    # Add more columns here as needed:
    # 'in_cooling_fuel',
    # 'in_lighting_type',
    # 'in_appliance_type',
]

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
        
        # Create county summary
        print("📊 Creating county-level summaries...")
        county_summary = create_county_summary(df)
        
        # Create SQLite connection
        print("🗄️ Creating SQLite database...")
        conn = sqlite3.connect(db_file)
        
        # Write county summary to SQLite
        print("💾 Writing county summary to SQLite...")
        county_summary.to_sql('county_summary', conn, if_exists='replace', index=False)
        
        # Create indexes for better query performance
        print("⚡ Creating indexes...")
        cursor = conn.cursor()
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_county_id ON county_summary (in_county)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_county_state ON county_summary (in_county_name, in_state)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_state ON county_summary (in_state)")
        
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
    Create county-level summary with essential aggregated data
    """
    print("📋 Creating county summary...")
    
    # Group by county and create summary
    county_groups = df.groupby(['in_county', 'in_county_name', 'in_state'])
    
    summary_data = []
    
    # Create progress bar
    pbar = tqdm(county_groups, desc="Processing counties", unit="county")
    
    for (county_id, county_name, state), group in pbar:
        state_fips = county_id[1:3]     # '01'
        county_fips = county_id[4:7]    # '000'
        fips = f'0500000US{state_fips}{county_fips}'
        
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
        
        
        
        # Generate distribution data for all configured columns
        distribution_data = {}
        
        for col in distribution_columns:
            if col in group.columns:
                try:
                    value_counts = group[col].value_counts()
                    distribution_data[f"{col}_dist"] = ','.join([f"{k}:{v}" for k, v in value_counts.items()])
                except Exception as e:
                    distribution_data[f"{col}_dist"] = None
            else:
                distribution_data[f"{col}_dist"] = None
        
        # Most common values (for quick reference)
        most_common_building_type = None
        if 'in_geometry_building_type_recs' in group.columns:
            try:
                most_common_building_type = group['in_geometry_building_type_recs'].mode().iloc[0] if len(group['in_geometry_building_type_recs'].mode()) > 0 else None
            except:
                most_common_building_type = None
        
        most_common_heating_fuel = None
        if 'in_heating_fuel' in group.columns:
            try:
                most_common_heating_fuel = group['in_heating_fuel'].mode().iloc[0] if len(group['in_heating_fuel'].mode()) > 0 else None
            except:
                most_common_heating_fuel = None
        
        most_common_water_heater_fuel = None
        if 'in_water_heater_fuel' in group.columns:
            try:
                most_common_water_heater_fuel = group['in_water_heater_fuel'].mode().iloc[0] if len(group['in_water_heater_fuel'].mode()) > 0 else None
            except:
                most_common_water_heater_fuel = None
        
        # Create summary row
        summary_row = {
            'in_county': county_id,
            'fips': fips,
            'in_county_name': county_name,
            'in_state': state,
            'building_count': building_count,
            'weighted_count': weighted_count,
            'avg_floor_area': avg_floor_area,
            'avg_vintage': avg_vintage,
            'avg_electricity_kwh': avg_electricity_kwh,
            'avg_electric_bill': avg_electric_bill,
            'avg_energy_burden': avg_energy_burden,
            'most_common_building_type': most_common_building_type,
            'most_common_heating_fuel': most_common_heating_fuel,
            'most_common_water_heater_fuel': most_common_water_heater_fuel
        }
        
        # Add all distribution data to summary row
        summary_row.update(distribution_data)
        
        summary_data.append(summary_row)
    
    # Create DataFrame
    summary_df = pd.DataFrame(summary_data)
    
    print(f"✅ Created summary for {len(summary_df)} counties")
    print(f"📊 Summary table columns ({len(summary_df.columns)} total):")
    
    # Group columns by type for better readability
    basic_cols = ['in_county', 'in_county_name', 'in_state', 'building_count', 'weighted_count']
    avg_cols = [col for col in summary_df.columns if col.startswith('avg_')]
    most_common_cols = [col for col in summary_df.columns if col.startswith('most_common_')]
    dist_cols = [col for col in summary_df.columns if col.endswith('_dist')]
    
    print(f"   📍 Basic columns ({len(basic_cols)}): {', '.join(basic_cols)}")
    print(f"   📊 Average columns ({len(avg_cols)}): {', '.join(avg_cols)}")
    print(f"   🏆 Most common columns ({len(most_common_cols)}): {', '.join(most_common_cols)}")
    print(f"   📈 Distribution columns ({len(dist_cols)}): {', '.join(dist_cols)}")
    
    return summary_df

if __name__ == "__main__":
    # Convert parquet to SQLite with county summaries
    success = convert_parquet_to_sqlite()
    
    if success:
        print("\n🎉 Conversion complete! The SQLite database contains county-level summaries for the dashboard.")
        print("📊 The county_summary table includes:")
        print("   • Building counts and demographics")
        print("   • Energy consumption and cost metrics")
        print("   • Most common building types and fuel types")
        print("   • Average building characteristics")
        print("   • Distribution data for: building types, heating fuel, water heater fuel, and vintage")
    else:
        print("\n❌ Conversion failed. Please check the error messages above.") 