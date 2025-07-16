#!/usr/bin/env python3
"""
ResStock Dashboard with Side-by-Side County Comparison
Each county gets its own complete dashboard with map and all visualizations
"""

import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import json
import numpy as np

# Set page config
st.set_page_config(
    page_title="ResStock Dashboard - County Comparison",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title
st.title("🏠 ResStock Interactive County Comparison")
st.markdown("Compare building energy data between two counties with separate dashboards")

@st.cache_data
def load_counties_geojson():
    """Load the single counties GeoJSON file"""
    try:
        with open('counties.geojson', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error("❌ counties.geojson file not found!")
        return None
    except Exception as e:
        st.error(f"❌ Error loading counties.geojson: {e}")
        return None


@st.cache_data
def get_county_stats():
    """Get county statistics from summary table"""
    conn = sqlite3.connect('resstock.db')
    query = "SELECT * FROM county_summary ORDER BY in_county_name, in_state"
    stats = pd.read_sql_query(query, conn)
    conn.close()
    return stats

@st.cache_data
def get_county_summary(county_name, state):
    """Get summary data for a specific county"""
    conn = sqlite3.connect('resstock.db')
    query = """
    SELECT * FROM county_summary 
    WHERE in_county_name = ? AND in_state = ?
    """
    data = pd.read_sql_query(query, conn, params=[county_name, state])
    conn.close()
    return data

def parse_distribution(dist_str):
    """Parse distribution string into pandas Series for plotting"""
    # Handle pandas Series input
    if hasattr(dist_str, 'iloc'):
        dist_str = dist_str.iloc[0]
    
    if pd.isna(dist_str) or dist_str is None or dist_str == '':
        return pd.Series()
    
    try:
        # Parse "type1:count1,type2:count2" format
        items = dist_str.split(',')
        data = {}
        for item in items:
            if ':' in item:
                key, value = item.split(':', 1)
                data[key.strip()] = int(value.strip())
        return pd.Series(data)
    except:
        return pd.Series()

def get_distribution_config():
    """Configuration for distribution columns - update this to add new columns"""
    return {
        'in_geometry_building_type_recs': {
            'title': 'Building Types',
            'display_name': 'Building Type Distribution'
        },
        'in_heating_fuel': {
            'title': 'Heating Fuel',
            'display_name': 'Heating Fuel Distribution'
        },
        'in_water_heater_fuel': {
            'title': 'Water Heater Fuel',
            'display_name': 'Water Heater Fuel Distribution'
        },
        'in_vintage': {
            'title': 'Building Vintage',
            'display_name': 'Building Vintage Distribution'
        },
        # Add new columns here:
        # 'in_cooling_fuel': {
        #     'title': 'Cooling Fuel',
        #     'display_name': 'Cooling Fuel Distribution'
        # },
        # 'in_lighting_type': {
        #     'title': 'Lighting Type',
        #     'display_name': 'Lighting Type Distribution'
        # },
    }



def display_county_dashboard(county_stats, selected_state_abbrev, selected_state, display_state, 
                           county_selector_label, county_selector_key, map_key):
    """Display a complete county dashboard with map and all visualizations"""
    
    # Initialize selected_county_full to prevent UnboundLocalError
    selected_county_full = None
    
    # County selector (filtered by selected state)
    state_county_stats = county_stats[county_stats['in_state'] == selected_state_abbrev]
    state_counties = state_county_stats[['in_county_name', 'in_state']].drop_duplicates()
    
    if len(state_counties) == 0:
        st.error(f"No counties found for {selected_state_abbrev} ({selected_state})")
        return None
    
    # Create county map using the counties.geojson file
    try:
        # Load counties GeoJSON data
        counties_geojson = load_counties_geojson()
        if counties_geojson is None:
            st.error("❌ Failed to load counties.geojson file")
            return None
        
        # Create the choropleth map using county IDs
        map_fig = px.choropleth(
            state_county_stats,
            geojson=counties_geojson,
            locations='fips', 
            featureidkey='properties.GEO_ID',
            color='building_count',
            hover_name='in_county_name',
            hover_data=['in_state', 'building_count', 'weighted_count'],
            color_continuous_scale='YlOrRd',
            title=f"Building Count by County - {display_state}",
            labels={'building_count': 'Building Count', 'weighted_count': 'Weighted Count'}
        )
        
        map_fig.update_geos(
            scope="usa",
            showland=True,
            landcolor="lightgray",
            showocean=True,
            oceancolor="lightblue",
            showlakes=True,
            lakecolor="lightblue"
        )
        
        map_fig.update_layout(
            height=400,
            margin={"r":0,"t":30,"l":0,"b":0}
        )
        
        st.plotly_chart(map_fig, use_container_width=True, key=f"map_{map_key}")
    except Exception as e:
        st.error(f"❌ Map Error: {e}")
        st.info("Note: County mapping requires county_id values in the 'in_county' column that match the GeoJSON properties")
    
    # County dropdown below the map
    county_options = [f"{row['in_county_name']}, {row['in_state']}" for _, row in state_counties.iterrows()]
    
    # Set default index based on the dashboard side and state
    default_index = 0
    if map_key == "county1" and selected_state_abbrev == "CA":
        # Default to Alameda County for County 1 in CA
        try:
            default_index = county_options.index("Alameda County, CA")
        except ValueError:
            default_index = 0
    elif map_key == "county2" and selected_state_abbrev == "GA":
        # Default to Fulton County for County 2 in GA
        try:
            default_index = county_options.index("Fulton County, GA")
        except ValueError:
            default_index = 0
    
    selected_county_full = st.selectbox(
        county_selector_label,
        options=sorted(county_options),
        index=default_index,
        key=county_selector_key
    )
    
    if selected_county_full is None:
        st.error("No county selected. Please check the state filter.")
        return None
    
    # Extract county name from selection
    selected_county = selected_county_full.split(", ")[0]
    selected_state_from_selection = selected_county_full.split(", ")[1]
    
    # Show county info
    county_data = county_stats[
        (county_stats['in_county_name'] == selected_county) & 
        (county_stats['in_state'] == selected_state_abbrev)
    ].iloc[0]
    
    st.info(f"**{selected_county}, {display_state}** (ID: {county_data['in_county']})")
    
    # Display key metrics
    col_1, col_2 = st.columns(2)
    
    with col_1:
        st.metric(
            label="# of Building Models",
            value=f"{county_data['building_count']:,}",
            help="Number of buildings in the dataset for this county"
        )
    
    with col_2:
        st.metric(
            label="# of Buildings Represented",
            value=f"{county_data['weighted_count']:,.0f}",
            help="Weighted count representing actual building population"
        )
    
    # Get county summary data
    county_summary = get_county_summary(selected_county, display_state)
    
    if county_summary is not None:
        # Create distribution charts
        st.subheader("📊 Distribution Charts")
        
        # First row: Building Type and Vintage
        col_dist1, col_dist2 = st.columns(2)
        
        with col_dist1:
            # Building Type Distribution
            dist_key = 'in_geometry_building_type_recs_dist'
            if dist_key in county_summary:
                dist_data = parse_distribution(county_summary[dist_key])
                if len(dist_data) > 0:
                    fig = px.pie(
                        values=dist_data.values,
                        names=dist_data.index,
                        title="Building Type Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True, key=f"{map_key}_building_type")
                else:
                    st.info("Building Type data not available")
            else:
                st.info("Building Type data not available")
        
        with col_dist2:
            # Vintage Distribution (Bar Chart)
            dist_key = 'in_vintage_dist'
            if dist_key in county_summary:
                dist_data = parse_distribution(county_summary[dist_key])
                if len(dist_data) > 0:
                    # Sort by NREL building vintage order
                    nrel_vintage_order = [
                        "<1940",
                        "1940s",
                        "1950s",
                        "1960s",
                        "1970s",
                        "1980s",
                        "1990s",
                        "2000s",
                        "2010s",
                    ]
                    
                    # Reorder the data according to NREL vintage order
                    sorted_data = {}
                    for vintage in nrel_vintage_order:
                        if vintage in dist_data.index:
                            sorted_data[vintage] = dist_data[vintage]
                    
                    # Create sorted series
                    sorted_dist_data = pd.Series(sorted_data)
                    
                    fig = px.bar(
                        x=sorted_dist_data.index,
                        y=sorted_dist_data.values,
                        title="Building Vintage Distribution",
                        labels={'x': 'Vintage Year', 'y': 'Number of Buildings'}
                    )
                    st.plotly_chart(fig, use_container_width=True, key=f"{map_key}_vintage")
                else:
                    st.info("Vintage data not available")
            else:
                st.info("Vintage data not available")
        
        # Second row: Heating Fuel and Water Heater Fuel
        col_dist3, col_dist4 = st.columns(2)
        
        with col_dist3:
            # Heating Fuel Distribution
            dist_key = 'in_heating_fuel_dist'
            if dist_key in county_summary:
                dist_data = parse_distribution(county_summary[dist_key])
                if len(dist_data) > 0:
                    fig = px.pie(
                        values=dist_data.values,
                        names=dist_data.index,
                        title="Heating Fuel Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True, key=f"{map_key}_heating_fuel")
                else:
                    st.info("Heating Fuel data not available")
            else:
                st.info("Heating Fuel data not available")
        
        with col_dist4:
            # Water Heater Fuel Distribution
            dist_key = 'in_water_heater_fuel_dist'
            if dist_key in county_summary:
                dist_data = parse_distribution(county_summary[dist_key])
                if len(dist_data) > 0:
                    fig = px.pie(
                        values=dist_data.values,
                        names=dist_data.index,
                        title="Water Heater Fuel Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True, key=f"{map_key}_water_heater_fuel")
                else:
                    st.info("Water Heater Fuel data not available")
            else:
                st.info("Water Heater Fuel data not available")
        
        # Energy metrics
        st.subheader("🔍 Energy Insights")
        
        col4_1, col4_2, col4_3 = st.columns(3)
        
        with col4_1:
            st.write("**Energy Consumption (kWh)**")
            if pd.notna(county_summary['avg_electricity_kwh'].iloc[0]) and county_summary['avg_electricity_kwh'].iloc[0] is not None:
                st.metric("Avg Electricity", f"{county_summary['avg_electricity_kwh'].iloc[0]:,.0f}")
            else:
                st.metric("Avg Electricity", "N/A")
        
        with col4_2:
            st.write("**Energy Costs ($)**")
            if pd.notna(county_summary['avg_electric_bill'].iloc[0]) and county_summary['avg_electric_bill'].iloc[0] is not None:
                st.metric("Avg Electric Bill", f"${county_summary['avg_electric_bill'].iloc[0]:.0f}")
            else:
                st.metric("Avg Electric Bill", "N/A")
        
        with col4_3:
            st.write("**Energy Burden (%)**")
            if pd.notna(county_summary['avg_energy_burden'].iloc[0]) and county_summary['avg_energy_burden'].iloc[0] is not None:
                st.metric("Avg Energy Burden", f"{county_summary['avg_energy_burden'].iloc[0]:.1f}%")
            else:
                st.metric("Avg Energy Burden", "N/A")
        
        # Energy Characteristics
        st.subheader("⚡ Energy Characteristics")
        
        if pd.notna(county_summary['most_common_heating_fuel'].iloc[0]) and county_summary['most_common_heating_fuel'].iloc[0] is not None:
            st.metric("Primary Heating Fuel", county_summary['most_common_heating_fuel'].iloc[0])
        else:
            st.metric("Primary Heating Fuel", "N/A")
        
        # County summary table
        st.subheader(f"📋 County Summary - {selected_county}, {display_state}")
        
        # Create a summary table
        summary_data = {
            'Metric': [
                'Total Buildings',
                'Weighted Buildings', 
                'Average Floor Area (sq ft)',
                'Average Vintage',
                'Average Electricity (kWh)',
                'Average Electric Bill ($)',
                'Average Energy Burden (%)',
                'Most Common Building Type',
                'Most Common Heating Fuel',
                'Most Common Water Heater Fuel'
            ],
            'Value': [
                f"{county_summary['building_count'].iloc[0]:,}",
                f"{county_summary['weighted_count'].iloc[0]:,.0f}",
                f"{county_summary['avg_floor_area'].iloc[0]:,.0f}" if pd.notna(county_summary['avg_floor_area'].iloc[0]) and county_summary['avg_floor_area'].iloc[0] is not None else "N/A",
                f"{county_summary['avg_vintage'].iloc[0]:.0f}" if pd.notna(county_summary['avg_vintage'].iloc[0]) and county_summary['avg_vintage'].iloc[0] is not None else "N/A",
                f"{county_summary['avg_electricity_kwh'].iloc[0]:,.0f}" if pd.notna(county_summary['avg_electricity_kwh'].iloc[0]) and county_summary['avg_electricity_kwh'].iloc[0] is not None else "N/A",
                f"{county_summary['avg_electric_bill'].iloc[0]:.0f}" if pd.notna(county_summary['avg_electric_bill'].iloc[0]) and county_summary['avg_electric_bill'].iloc[0] is not None else "N/A",
                f"{county_summary['avg_energy_burden'].iloc[0]:.1f}%" if pd.notna(county_summary['avg_energy_burden'].iloc[0]) and county_summary['avg_energy_burden'].iloc[0] is not None else "N/A",
                county_summary['most_common_building_type'].iloc[0] if pd.notna(county_summary['most_common_building_type'].iloc[0]) else "N/A",
                county_summary['most_common_heating_fuel'].iloc[0] if pd.notna(county_summary['most_common_heating_fuel'].iloc[0]) else "N/A",
                county_summary['most_common_water_heater_fuel'].iloc[0] if pd.notna(county_summary['most_common_water_heater_fuel'].iloc[0]) else "N/A"
            ]
        }
        
        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True, key=f"table_{map_key}")
    else:
        st.warning("No data available for selected county")
    
    return selected_county, display_state

def main():
    # Check if database exists
    try:
        conn = sqlite3.connect('resstock.db')
        conn.close()
    except Exception as e:
        st.error(f"❌ Database Error: {e}")
        st.error("❌ SQLite database not found! Please run `python convert_to_sqlite.py` first.")
        st.stop()
    
    # Load data
    try:
        county_stats = get_county_stats()
    except Exception as e:
        st.error(f"❌ County Stats Error: {e}")
        st.stop()
    
    # State lookup tables
    state_lookup = pd.read_csv('state_lookup.csv')
    state_abbrev_to_name = dict(zip(state_lookup['state_abbrev'], state_lookup['state_name']))
    state_name_to_abbrev = dict(zip(state_lookup['state_name'], state_lookup['state_abbrev']))
    
    # Get unique states from data and convert to abbreviations
    unique_states = sorted(county_stats['in_state'].unique())
    state_abbrevs = [state_name_to_abbrev.get(state, state) for state in unique_states]
    
    # Create two columns for completely separate dashboards
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏠 County 1 Dashboard")
        
        # Separate state filter for County 1
        selected_state_abbrev_1 = st.selectbox(
            "Select State for County 1:",
            options=state_abbrevs,
            index=state_abbrevs.index('CA') if 'CA' in state_abbrevs else 0,
            key="state1_selector"
        )
        
        # Convert back to full state name for filtering data
        selected_state_1 = state_abbrev_to_name.get(selected_state_abbrev_1, selected_state_abbrev_1)
        
        # Check if we need to filter by abbreviation or full name
        if selected_state_abbrev_1 in county_stats['in_state'].values:
            # Data uses abbreviations
            display_state_1 = selected_state_abbrev_1
        else:
            # Data uses full names
            display_state_1 = selected_state_1
        
        st.write(f"📊 Showing counties in {display_state_1}")
        
        county1_data = display_county_dashboard(
            county_stats, 
            selected_state_abbrev_1, 
            selected_state_1, 
            display_state_1,
            f"Select County 1 in {selected_state_abbrev_1}:",
            "county1_selector",
            "county1"
        )
    
    with col2:
        st.subheader("🏠 County 2 Dashboard")
        
        # Separate state filter for County 2
        selected_state_abbrev_2 = st.selectbox(
            "Select State for County 2:",
            options=state_abbrevs,
            index=state_abbrevs.index('GA') if 'GA' in state_abbrevs else 0,
            key="state2_selector"
        )
        
        # Convert back to full state name for filtering data
        selected_state_2 = state_abbrev_to_name.get(selected_state_abbrev_2, selected_state_abbrev_2)
        
        # Check if we need to filter by abbreviation or full name
        if selected_state_abbrev_2 in county_stats['in_state'].values:
            # Data uses abbreviations
            display_state_2 = selected_state_abbrev_2
        else:
            # Data uses full names
            display_state_2 = selected_state_2
        
        st.write(f"📊 Showing counties in {display_state_2}")
        
        county2_data = display_county_dashboard(
            county_stats, 
            selected_state_abbrev_2, 
            selected_state_2, 
            display_state_2,
            f"Select County 2 in {selected_state_abbrev_2}:",
            "county2_selector",
            "county2"
        )
    
    # Add comparison section if both counties are selected
    if county1_data and county2_data:
        county1_name, county1_state = county1_data
        county2_name, county2_state = county2_data
        
        st.subheader("🔍 Side-by-Side Comparison")
        
        # Get data for both counties
        county1_summary = get_county_summary(county1_name, county1_state)
        county2_summary = get_county_summary(county2_name, county2_state)
        
        if county1_summary is not None and county2_summary is not None:
            # Create comparison metrics
            comp_col1, comp_col2, comp_col3, comp_col4 = st.columns(4)
            
            with comp_col1:
                st.write("**Building Count Comparison**")
                st.metric(
                    f"{county1_name}",
                    f"{county1_summary['building_count'].iloc[0]:,}",
                    delta=f"{county1_summary['building_count'].iloc[0] - county2_summary['building_count'].iloc[0]:,}"
                )
                st.metric(
                    f"{county2_name}",
                    f"{county2_summary['building_count'].iloc[0]:,}",
                    delta=f"{county2_summary['building_count'].iloc[0] - county1_summary['building_count'].iloc[0]:,}"
                )
            
            with comp_col2:
                st.write("**Average Floor Area Comparison**")
                if (pd.notna(county1_summary['avg_floor_area'].iloc[0]) and county1_summary['avg_floor_area'].iloc[0] is not None and
                    pd.notna(county2_summary['avg_floor_area'].iloc[0]) and county2_summary['avg_floor_area'].iloc[0] is not None):
                    st.metric(
                        f"{county1_name}",
                        f"{county1_summary['avg_floor_area'].iloc[0]:,.0f} sq ft",
                        delta=f"{county1_summary['avg_floor_area'].iloc[0] - county2_summary['avg_floor_area'].iloc[0]:,.0f}"
                    )
                    st.metric(
                        f"{county2_name}",
                        f"{county2_summary['avg_floor_area'].iloc[0]:,.0f} sq ft",
                        delta=f"{county2_summary['avg_floor_area'].iloc[0] - county1_summary['avg_floor_area'].iloc[0]:,.0f}"
                    )
                else:
                    st.info("Floor area data not available for comparison")
            
            with comp_col3:
                st.write("**Average Electricity Comparison**")
                if (pd.notna(county1_summary['avg_electricity_kwh'].iloc[0]) and county1_summary['avg_electricity_kwh'].iloc[0] is not None and
                    pd.notna(county2_summary['avg_electricity_kwh'].iloc[0]) and county2_summary['avg_electricity_kwh'].iloc[0] is not None):
                    st.metric(
                        f"{county1_name}",
                        f"{county1_summary['avg_electricity_kwh'].iloc[0]:,.0f} kWh",
                        delta=f"{county1_summary['avg_electricity_kwh'].iloc[0] - county2_summary['avg_electricity_kwh'].iloc[0]:,.0f}"
                    )
                    st.metric(
                        f"{county2_name}",
                        f"{county2_summary['avg_electricity_kwh'].iloc[0]:,.0f} kWh",
                        delta=f"{county2_summary['avg_electricity_kwh'].iloc[0] - county1_summary['avg_electricity_kwh'].iloc[0]:,.0f}"
                    )
                else:
                    st.info("Electricity data not available for comparison")
            
            with comp_col4:
                st.write("**Average Energy Burden Comparison**")
                if (pd.notna(county1_summary['avg_energy_burden'].iloc[0]) and county1_summary['avg_energy_burden'].iloc[0] is not None and
                    pd.notna(county2_summary['avg_energy_burden'].iloc[0]) and county2_summary['avg_energy_burden'].iloc[0] is not None):
                    st.metric(
                        f"{county1_name}",
                        f"{county1_summary['avg_energy_burden'].iloc[0]:.1f}%",
                        delta=f"{county1_summary['avg_energy_burden'].iloc[0] - county2_summary['avg_energy_burden'].iloc[0]:.1f}"
                    )
                    st.metric(
                        f"{county2_name}",
                        f"{county2_summary['avg_energy_burden'].iloc[0]:.1f}%",
                        delta=f"{county2_summary['avg_energy_burden'].iloc[0] - county1_summary['avg_energy_burden'].iloc[0]:.1f}"
                    )
                else:
                    st.info("Energy burden data not available for comparison")

if __name__ == "__main__":
    main() 