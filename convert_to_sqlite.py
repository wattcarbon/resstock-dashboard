#!/usr/bin/env python3
"""
Script to convert ResStock parquet file to SQLite database with county and building type summaries
This creates aggregated data for visualizations without storing raw building data.
"""

import pandas as pd
import sqlite3
import os
import requests
import json
from pathlib import Path
from tqdm import tqdm
from io import StringIO
import typer

app = typer.Typer()

# Distribution data (compact format for visualizations)
# Configuration: Add column names here to automatically include them in distributions
distribution_columns = [
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
    Convert parquet file to SQLite database with county and building type summaries
    
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
        
        # Create county summary (for backward compatibility)
        print("📊 Creating county-level summaries...")
        county_summary = create_county_summary(df)
        
        # Create county and building type summary
        print("📊 Creating county and building type summaries...")
        county_building_summary = create_county_building_summary(df)
        
        # Create SQLite connection
        print("🗄️ Creating SQLite database...")
        conn = sqlite3.connect(db_file)
        
        # Write county summary to SQLite
        print("💾 Writing county summary to SQLite...")
        county_summary.to_sql('county_summary', conn, if_exists='replace', index=False)
        
        # Write county and building type summary to SQLite
        print("💾 Writing county and building type summary to SQLite...")
        county_building_summary.to_sql('county_building_summary', conn, if_exists='replace', index=False)
        
        # Create indexes for better query performance
        print("⚡ Creating indexes...")
        cursor = conn.cursor()
        
        # Indexes for county_summary table
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_county_id ON county_summary (in_county)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_county_state ON county_summary (in_county_name, in_state)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_state ON county_summary (in_state)")
        
        # Indexes for county_building_summary table
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cb_county_id ON county_building_summary (in_county)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cb_county_state ON county_building_summary (in_county_name, in_state)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cb_state ON county_building_summary (in_state)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cb_building_type ON county_building_summary (in_geometry_building_type_recs)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cb_county_building ON county_building_summary (in_county_name, in_state, in_geometry_building_type_recs)")
        
        conn.commit()
        
        # Get some basic stats
        cursor.execute("SELECT COUNT(*) FROM county_summary")
        total_counties = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT in_state) FROM county_summary")
        total_states = cursor.fetchone()[0]
        
        cursor.execute("SELECT SUM(building_count) FROM county_summary")
        total_buildings = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM county_building_summary")
        total_county_building_combinations = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT in_geometry_building_type_recs) FROM county_building_summary")
        total_building_types = cursor.fetchone()[0]
        
        # Get sample counties
        cursor.execute("SELECT in_county_name, in_state, building_count FROM county_summary ORDER BY building_count DESC LIMIT 5")
        sample_counties = cursor.fetchall()
        
        # Get sample building types
        cursor.execute("SELECT in_geometry_building_type_recs, COUNT(*) as count FROM county_building_summary GROUP BY in_geometry_building_type_recs ORDER BY count DESC LIMIT 5")
        sample_building_types = cursor.fetchall()
        
        conn.close()
        
        print("✅ Successfully converted to SQLite!")
        print("📊 Database stats:")
        print(f"   - Total counties: {total_counties:,}")
        print(f"   - Total states: {total_states}")
        print(f"   - Total buildings: {total_buildings:,}")
        print(f"   - Total county-building type combinations: {total_county_building_combinations:,}")
        print(f"   - Total building types: {total_building_types}")
        print("   - Top counties by building count:")
        for county, state, count in sample_counties:
            print(f"     • {county}, {state}: {count:,} buildings")
        print("   - Building types found:")
        for building_type, count in sample_building_types:
            print(f"     • {building_type}: {count:,} combinations")
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

def create_county_building_summary(df):
    """
    Create county and building type level summary with essential aggregated data
    """
    print("📋 Creating county and building type summary...")
    
    # Group by county and building type
    county_building_groups = df.groupby(['in_county', 'in_county_name', 'in_state', 'in_geometry_building_type_recs'])
    
    summary_data = []
    
    # Create progress bar
    pbar = tqdm(county_building_groups, desc="Processing county-building combinations", unit="combination")
    
    for (county_id, county_name, state, building_type), group in pbar:
        state_fips = county_id[1:3]     # '01'
        county_fips = county_id[4:7]    # '000'
        fips = f'0500000US{state_fips}{county_fips}'
        
        # Update progress bar description with current county and building type
        pbar.set_description(f"Processing {county_name}, {state} - {building_type}")
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
            'in_geometry_building_type_recs': building_type,
            'building_count': building_count,
            'weighted_count': weighted_count,
            'avg_floor_area': avg_floor_area,
            'avg_vintage': avg_vintage,
            'avg_electricity_kwh': avg_electricity_kwh,
            'avg_electric_bill': avg_electric_bill,
            'avg_energy_burden': avg_energy_burden,
            'most_common_heating_fuel': most_common_heating_fuel,
            'most_common_water_heater_fuel': most_common_water_heater_fuel
        }
        
        # Add all distribution data to summary row
        summary_row.update(distribution_data)
        
        summary_data.append(summary_row)
    
    # Create DataFrame
    summary_df = pd.DataFrame(summary_data)
    
    print(f"✅ Created summary for {len(summary_df)} county-building type combinations")
    print(f"📊 Summary table columns ({len(summary_df.columns)} total):")
    
    # Group columns by type for better readability
    basic_cols = ['in_county', 'in_county_name', 'in_state', 'in_geometry_building_type_recs', 'building_count', 'weighted_count']
    avg_cols = [col for col in summary_df.columns if col.startswith('avg_')]
    most_common_cols = [col for col in summary_df.columns if col.startswith('most_common_')]
    dist_cols = [col for col in summary_df.columns if col.endswith('_dist')]
    
    print(f"   📍 Basic columns ({len(basic_cols)}): {', '.join(basic_cols)}")
    print(f"   📊 Average columns ({len(avg_cols)}): {', '.join(avg_cols)}")
    print(f"   🏆 Most common columns ({len(most_common_cols)}): {', '.join(most_common_cols)}")
    print(f"   📈 Distribution columns ({len(dist_cols)}): {', '.join(dist_cols)}")
    
    return summary_df

def create_loadshape_summaries(db_file='resstock.db', upgrades_file='upgrades_lookup.json', state_filter=None, upgrade_filter=None):
    """
    Create loadshape summaries by fetching data from S3 for all upgrade/state/building type combinations
    
    Args:
        db_file (str): Path to the SQLite database
        upgrades_file (str): Path to the upgrades lookup JSON file
        state_filter (list): List of states to process (None for all states)
        upgrade_filter (list): List of upgrades to process (None for all upgrades)
    """
    print("📊 Creating loadshape summaries...")
    
    # Load upgrades lookup
    try:
        with open(upgrades_file, 'r') as f:
            upgrades_lookup = json.load(f)
    except FileNotFoundError:
        print(f"❌ {upgrades_file} file not found!")
        return False
    except Exception as e:
        print(f"❌ Error loading {upgrades_file}: {e}")
        return False
    
    # Building type mapping
    building_type_map = {
        'Single-Family Detached': 'single-family_detached',
        'Single-Family Attached': 'single-family_attached', 
        'Mobile Home': 'mobile_home',
        'Multi-Family with 2 - 4 Units': 'multi-family_with_2_-_4_units',
        'Multi-Family with 5+ Units': 'multi-family_with_5plus_units'
    }
    
    # Get unique states and building types from the database
    conn = sqlite3.connect(db_file)
    states = pd.read_sql_query("SELECT DISTINCT in_state FROM county_summary ORDER BY in_state", conn)
    building_types = pd.read_sql_query("SELECT DISTINCT in_geometry_building_type_recs FROM county_building_summary ORDER BY in_geometry_building_type_recs", conn)
    conn.close()
    
    states_list = states['in_state'].tolist()
    building_types_list = building_types['in_geometry_building_type_recs'].tolist()
    upgrades_list = list(upgrades_lookup.keys())
    
    # Filter states if specified
    if state_filter:
        # Convert state filter to uppercase for case-insensitive matching
        state_filter_upper = [s.upper() for s in state_filter]
        states_list = [state for state in states_list if state.upper() in state_filter_upper]
        
        if not states_list:
            print(f"❌ No matching states found for: {', '.join(state_filter)}")
            print(f"Available states: {', '.join(states['in_state'].tolist())}")
            return False
        
        print(f"📋 Filtering to states: {', '.join(states_list)}")
    else:
        print(f"📋 Processing all {len(states_list)} states")
    
    # Filter upgrades if specified
    if upgrade_filter:
        # Validate that all specified upgrades exist
        available_upgrades = set(int(k) for k in upgrades_lookup.keys())
        requested_upgrades = set(upgrade_filter)
        
        if not requested_upgrades.issubset(available_upgrades):
            invalid_upgrades = requested_upgrades - available_upgrades
            print(f"❌ Invalid upgrades specified: {', '.join(map(str, invalid_upgrades))}")
            print(f"Available upgrades: {', '.join(map(str, sorted(available_upgrades)))}")
            return False
        
        upgrades_list = [str(upgrade) for upgrade in requested_upgrades]
        print(f"📋 Filtering to upgrades: {', '.join(upgrades_list)}")
    else:
        print(f"📋 Processing all {len(upgrades_list)} upgrades")
    
    print(f"📋 Processing {len(states_list)} states, {len(building_types_list)} building types, {len(upgrades_list)} upgrades")
    print(f"   Total combinations: {len(states_list) * len(building_types_list) * len(upgrades_list):,}")
    
    loadshape_data = []
    total_combinations = len(states_list) * len(building_types_list) * len(upgrades_list)
    
    # Create progress bar
    pbar = tqdm(total=total_combinations, desc="Processing loadshape data", unit="combination")
    
    for state in states_list:
        for building_type in building_types_list:
            for upgrade in upgrades_list:
                try:
                    # Update progress bar description
                    pbar.set_description(f"Processing {state} - {building_type} - upgrade={upgrade}")
                    
                    # Get filename for building type
                    building_type_file = building_type_map.get(building_type, 'single-family_detached')
                    
                    # Construct URL with URL encoding
                    url = f"https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_amy2018_release_2/timeseries_aggregates/by_state/upgrade%3D{upgrade}/state%3D{state}/up{int(upgrade):02d}-{state.lower()}-{building_type_file}.csv"
                    
                    # Fetch data
                    response = requests.get(url, timeout=30)
                    response.raise_for_status()
                    
                    # Parse CSV data
                    df = pd.read_csv(StringIO(response.text))
                    
                    # Convert timestamp to datetime
                    df.index = pd.to_datetime(df['timestamp'])
                    
                    # Get columns that contain 'total' or 'emissions' in their name
                    total_columns = [col for col in df.columns if 'total' in col.lower() and 'savings' not in col]
                    emissions_columns = [col for col in df.columns if 'emissions' in col.lower() and '_15.' in col]
                    
                    # Combine both types of columns
                    selected_columns = total_columns + emissions_columns
                    
                    if selected_columns:
                        # Calculate hourly averages for selected columns
                        hourly_avg = df[selected_columns].groupby(df.index.hour).mean()
                        
                        # Create data for each hour and column
                        for hour in range(24):
                            for column in selected_columns:
                                loadshape_data.append({
                                    'state': state,
                                    'building_type': building_type,
                                    'upgrade': int(upgrade),
                                    'hour_of_day': hour,
                                    'column_name': column,
                                    'avg_value': hourly_avg.loc[hour, column] if hour in hourly_avg.index else None
                                })
                    
                    pbar.update(1)
                    
                except Exception as e:
                    print(f"\n⚠️ Warning: Failed to fetch data for {state} - {building_type} - upgrade={upgrade}: {e}")
                    pbar.update(1)
                    continue
    
    pbar.close()
    
    if loadshape_data:
        # Create DataFrame
        loadshape_df = pd.DataFrame(loadshape_data)
        
        # Save to database
        conn = sqlite3.connect(db_file)
        loadshape_df.to_sql('loadshape_summary', conn, if_exists='replace', index=False)
        
        # Create indexes
        cursor = conn.cursor()
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loadshape_state ON loadshape_summary (state)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loadshape_building_type ON loadshape_summary (building_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loadshape_upgrade ON loadshape_summary (upgrade)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loadshape_hour ON loadshape_summary (hour_of_day)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loadshape_state_building_upgrade ON loadshape_summary (state, building_type, upgrade)")
        
        conn.commit()
        
        # Get stats
        cursor.execute("SELECT COUNT(*) FROM loadshape_summary")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT state) FROM loadshape_summary")
        total_states_processed = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT building_type) FROM loadshape_summary")
        total_building_types_processed = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT upgrade) FROM loadshape_summary")
        total_upgrades_processed = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT column_name) FROM loadshape_summary")
        total_columns_processed = cursor.fetchone()[0]
        
        conn.close()
        
        print(f"✅ Successfully created loadshape summaries!")
        print(f"📊 Loadshape summary stats:")
        print(f"   - Total records: {total_records:,}")
        print(f"   - States processed: {total_states_processed}")
        print(f"   - Building types processed: {total_building_types_processed}")
        print(f"   - Upgrades processed: {total_upgrades_processed}")
        print(f"   - Columns processed: {total_columns_processed}")
        print(f"   - Hours per combination: 24")
        
        return True
    else:
        print("❌ No loadshape data was successfully processed")
        return False

@app.command()
def counties(
    parquet_file: str = typer.Option('baseline.parquet', '--parquet-file', '-p', help="Path to the parquet file"),
    db_file: str = typer.Option('resstock.db', '--db-file', '-d', help="Path to the output SQLite database")
):
    """Convert parquet file to SQLite database with county and building type summaries"""
    success = convert_parquet_to_sqlite(parquet_file, db_file)
    
    if success:
        print("\n🎉 County conversion complete! The SQLite database contains county-level and county-building type summaries for the dashboard.")
        print("📊 The county_summary table includes:")
        print("   • Building counts and demographics")
        print("   • Energy consumption and cost metrics")
        print("   • Most common building types and fuel types")
        print("   • Average building characteristics")
        print("   • Distribution data for: heating fuel, water heater fuel, and vintage")
        print("📊 The county_building_summary table includes:")
        print("   • Building counts and demographics by building type")
        print("   • Energy consumption and cost metrics by building type")
        print("   • Most common fuel types by building type")
        print("   • Average building characteristics by building type")
        print("   • Distribution data by building type")
    else:
        print("\n❌ County conversion failed. Please check the error messages above.")
        raise typer.Exit(1)

def create_building_lookup(parquet_file='baseline.parquet', db_file='resstock_building_lookup.db'):
    """
    Create a building lookup table with bldg_id, state, and building_type from parquet file
    
    Args:
        parquet_file (str): Path to the parquet file
        db_file (str): Path to the SQLite database
    """
    print(f"📋 Creating building lookup table from {parquet_file}...")
    
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
        
        print(f"✅ Loaded {len(df):,} rows")
        
        # Extract only the columns we need
        # Check which columns exist and add them
        
        # Create lookup dataframe
        building_lookup = df[['bldg_id', 'in_state', 'in_geometry_building_type_recs', 'in_county']].copy()
        
        # Rename columns to simpler names
        rename_map = {"bldg_id": "bldg_id", "in_state": "state", "in_geometry_building_type_recs": "building_type", "in_county": "county"}
        building_lookup = building_lookup.rename(columns=rename_map)
        
        # Connect to database
        print("🗄️ Writing to SQLite database...")
        conn = sqlite3.connect(db_file)
        
        # Write building lookup to SQLite
        building_lookup.to_sql('building_lookup', conn, if_exists='replace', index=False)
        
        # Create indexes for better query performance
        print("⚡ Creating indexes...")
        cursor = conn.cursor()
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_bldg_id ON building_lookup (bldg_id)")
        # Check if columns exist after renaming
        cursor.execute("PRAGMA table_info(building_lookup)")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]
        
        if 'state' in column_names:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_state ON building_lookup (state)")
        if 'building_type' in column_names:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_building_type ON building_lookup (building_type)")
        if 'state' in column_names and 'building_type' in column_names:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_state_building_type ON building_lookup (state, building_type)")
        
        conn.commit()
        
        # Get stats
        cursor.execute("SELECT COUNT(*) FROM building_lookup")
        total_buildings = cursor.fetchone()[0]
        
        total_states = 0
        total_building_types = 0
        if 'state' in column_names:
            cursor.execute("SELECT COUNT(DISTINCT state) FROM building_lookup")
            total_states = cursor.fetchone()[0]
        if 'building_type' in column_names:
            cursor.execute("SELECT COUNT(DISTINCT building_type) FROM building_lookup")
            total_building_types = cursor.fetchone()[0]
        
        conn.close()
        
        print("✅ Successfully created building lookup table!")
        print("📊 Building lookup stats:")
        print(f"   - Total buildings: {total_buildings:,}")
        print(f"   - Total states: {total_states}")
        print(f"   - Total building types: {total_building_types}")
        print(f"   - Database file: {db_file}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during building lookup creation: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

@app.command()
def building_lookup(
    parquet_file: str = typer.Option('baseline.parquet', '--parquet-file', '-p', help="Path to the parquet file"),
    db_file: str = typer.Option('resstock_building_lookup.db', '--db-file', '-d', help="Path to the SQLite database")
):
    """Create a building lookup table with bldg_id, state, and building_type from parquet file"""
    success = create_building_lookup(parquet_file, db_file)
    
    if success:
        print("\n🎉 Building lookup creation complete!")
        print("📊 The building_lookup table includes:")
        print("   • bldg_id - Building ID")
        print("   • state - State abbreviation")
        print("   • building_type - Building type")
        print("   • Indexed for fast querying")
    else:
        print("\n❌ Building lookup creation failed. Please check the error messages above.")
        raise typer.Exit(1)

@app.command()
def loadshape(
    db_file: str = typer.Option('resstock.db', '--db-file', '-d', help="Path to the SQLite database"),
    upgrades_file: str = typer.Option('upgrades_lookup.json', '--upgrades-file', '-u', help="Path to the upgrades lookup JSON file"),
    state: list[str] = typer.Option(None, '--state', '-s', help="States to process (can specify multiple times)"),
    upgrade: list[int] = typer.Option(None, '--upgrade', help="Upgrades to process (can specify multiple times)")
):
    """Create loadshape summaries by fetching data from S3 for all upgrade/state/building type combinations"""
    # Check if database exists
    if not os.path.exists(db_file):
        print(f"❌ Error: {db_file} not found!")
        print("Please run the counties command first to create the database.")
        raise typer.Exit(1)
    
    # Check if upgrades file exists
    if not os.path.exists(upgrades_file):
        print(f"❌ Error: {upgrades_file} not found!")
        raise typer.Exit(1)
    
    success = create_loadshape_summaries(db_file, upgrades_file, state, upgrade)
    
    if success:
        print("\n🎉 Loadshape conversion complete!")
        print("📊 The loadshape_summary table includes:")
        print("   • Hourly averages for all upgrade/state/building type combinations")
        print("   • Columns containing 'total' or 'emissions' in their name")
        print("   • 24 hours of data per combination")
        print("   • Indexed for fast querying")
    else:
        print("\n❌ Loadshape conversion failed. Please check the error messages above.")
        raise typer.Exit(1)

@app.command()
def all(
    parquet_file: str = typer.Option('baseline.parquet', '--parquet-file', '-p', help="Path to the parquet file"),
    db_file: str = typer.Option('resstock.db', '--db-file', '-d', help="Path to the output SQLite database"),
    upgrades_file: str = typer.Option('upgrades_lookup.json', '--upgrades-file', '-u', help="Path to the upgrades lookup JSON file"),
    state: list[str] = typer.Option(None, '--state', '-s', help="States to process for loadshape (can specify multiple times)")
):
    """Run both county and loadshape conversions in sequence"""
    # First run county conversion
    print("🔄 Step 1: Creating county tables...")
    county_success = convert_parquet_to_sqlite(parquet_file, db_file)
    
    if not county_success:
        print("❌ County conversion failed. Stopping.")
        raise typer.Exit(1)
    
    print("\n🎉 County conversion complete!")
    
    # Then run loadshape conversion
    print("\n🔄 Step 2: Creating loadshape tables...")
    loadshape_success = create_loadshape_summaries(db_file, upgrades_file, state)
    
    if not loadshape_success:
        print("❌ Loadshape conversion failed.")
        raise typer.Exit(1)
    
    print("\n🎉 All conversions complete!")
    print("📊 Database now contains:")
    print("   • county_summary table")
    print("   • county_building_summary table")
    print("   • loadshape_summary table")

if __name__ == "__main__":
    app() 