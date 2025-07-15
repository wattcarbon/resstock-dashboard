import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import json
import numpy as np

# Set page config
st.set_page_config(
    page_title="ResStock Dashboard",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title
st.title("🏠 ResStock Interactive County Map")
st.markdown("Select counties to explore building energy data")

@st.cache_data
def load_state_geojson(state_name):
    """Load state-specific GeoJSON data (pre-processed)"""
    try:
        # Get state FIPS code from lookup table using state abbreviation
        state_lookup = pd.read_csv('state_lookup.csv')
        state_fips = state_lookup[state_lookup['state_abbrev'] == state_name]['state_fips'].iloc[0]
        state_fips = str(state_fips).zfill(2)
        
        filename = f"states_geojson/{state_fips}.geojson"
        
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error(f"❌ GeoJSON files not found! Please run `python convert_shapefiles.py` first. {filename}")

@st.cache_data
def get_county_stats():
    """Get county statistics from summary table"""
    conn = sqlite3.connect('resstock.db')
    query = "SELECT * FROM county_summary ORDER BY in_county_name, in_state"
    stats = pd.read_sql_query(query, conn)
    conn.close()
    return stats

@st.cache_data
def get_county_data(county_name):
    """Get detailed data for a specific county"""
    conn = sqlite3.connect('resstock.db')
    query = """
    SELECT * FROM buildings 
    WHERE in_county_name = ?
    """
    data = pd.read_sql_query(query, conn, params=[county_name])
    conn.close()
    return data

def create_plotly_map(geojson_data, county_stats):
    """Create an interactive plotly map"""
    
    # Create the choropleth map using county IDs
    fig = px.choropleth(
        county_stats,
        geojson=geojson_data,
        locations='in_county', 
        featureidkey='properties.county_id',  # Match on GEOID
        color='building_count',
        hover_name='in_county_name',
        hover_data=['in_state', 'building_count', 'weighted_count'],
        color_continuous_scale='YlOrRd',
        title="Building Count by County",
        labels={'building_count': 'Building Count', 'weighted_count': 'Weighted Count'}
    )
    
    fig.update_geos(
        scope="usa",
        showland=True,
        landcolor="lightgray",
        showocean=True,
        oceancolor="lightblue",
        showlakes=True,
        lakecolor="lightblue"
    )
    
    fig.update_layout(
        height=600,
        margin={"r":0,"t":30,"l":0,"b":0}
    )
    
    return fig

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
    
    # State filter - convert to abbreviations for display
    state_lookup = pd.read_csv('state_lookup.csv')
    state_abbrev_to_name = dict(zip(state_lookup['state_abbrev'], state_lookup['state_name']))
    state_name_to_abbrev = dict(zip(state_lookup['state_name'], state_lookup['state_abbrev']))
    
    # Get unique states from data and convert to abbreviations
    unique_states = sorted(county_stats['in_state'].unique())
    state_abbrevs = [state_name_to_abbrev.get(state, state) for state in unique_states]
    
    selected_state_abbrev = st.selectbox(
        "Filter by state:",
        options=state_abbrevs,
        index=state_abbrevs.index('CA') if 'CA' in state_abbrevs else 0
    )
    
    # Convert back to full state name for filtering data
    selected_state = state_abbrev_to_name.get(selected_state_abbrev, selected_state_abbrev)
    
    # Check if we need to filter by abbreviation or full name
    if selected_state_abbrev in county_stats['in_state'].values:
        # Data uses abbreviations
        state_county_stats = county_stats[county_stats['in_state'] == selected_state_abbrev]
        display_state = selected_state_abbrev
    else:
        # Data uses full names
        state_county_stats = county_stats[county_stats['in_state'] == selected_state]
        display_state = selected_state
    
    st.write(f"📊 Showing {len(state_county_stats)} counties in {display_state}")
    
    # Create two columns
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("🗺️ Interactive County Map")
        
        # Create the map for selected state
        try:
            # Load state-specific GeoJSON
            state_geojson = load_state_geojson(selected_state_abbrev)
            if state_geojson is None:
                st.error(f"❌ Failed to load GeoJSON for {selected_state_abbrev}")
            else:
                map_fig = create_plotly_map(state_geojson, state_county_stats)
                st.plotly_chart(map_fig, use_container_width=True)
        except Exception as e:
            st.error(f"❌ Map Error: {e}")
        
        st.markdown("""
        **Map Instructions:**
        - **Color intensity** shows building count density
        - **Hover** to see county details
        - **Zoom and pan** to explore different regions
        """)
    
    with col2:
        st.subheader("📍 County Selection")
        
        # County selector (filtered by selected state)
        state_counties = state_county_stats[['in_county_name', 'in_state']].drop_duplicates()
        
        if len(state_counties) == 0:
            st.error(f"No counties found for {selected_state_abbrev} ({selected_state})")
            st.stop()
        
        county_options = [f"{row['in_county_name']}, {row['in_state']}" for _, row in state_counties.iterrows()]
        selected_county_full = st.selectbox(
            f"Select a county in {selected_state_abbrev}:",
            options=sorted(county_options),
            index=0
        )
        
        if selected_county_full is None:
            st.error("No county selected. Please check the state filter.")
            st.stop()
        
        # Extract county name from selection
        selected_county = selected_county_full.split(", ")[0]
        selected_state_from_selection = selected_county_full.split(", ")[1]
        
        # Show county info - use the same logic as above for state matching
        if selected_state_abbrev in county_stats['in_state'].values:
            # Data uses abbreviations
            county_data = county_stats[
                (county_stats['in_county_name'] == selected_county) & 
                (county_stats['in_state'] == selected_state_abbrev)
            ].iloc[0]
        else:
            # Data uses full names
            county_data = county_stats[
                (county_stats['in_county_name'] == selected_county) & 
                (county_stats['in_state'] == selected_state)
            ].iloc[0]
        
        st.info(f"**{selected_county}, {display_state}** (ID: {county_data['in_county']})")
        
        # Display key metrics
        col2_1, col2_2 = st.columns(2)
        
        with col2_1:
            st.metric(
                label="Total Buildings",
                value=f"{county_data['building_count']:,}",
                help="Number of buildings in the dataset for this county"
            )
        
        with col2_2:
            st.metric(
                label="Weighted Buildings",
                value=f"{county_data['weighted_count']:,.0f}",
                help="Weighted count representing actual building population"
            )
    
    # Get detailed county data
    county_df = get_county_data(selected_county)
    
    if len(county_df) > 0:
        # Create visualizations
        st.subheader("📊 Building Statistics")
        
        col3_1, col3_2 = st.columns(2)
        
        with col3_1:
            # Building types pie chart
            building_types = county_df['in_geometry_building_type_recs'].value_counts()
            fig1 = px.pie(
                values=building_types.values, 
                names=building_types.index,
                title="Building Types"
            )
            st.plotly_chart(fig1, use_container_width=True)
        
        with col3_2:
            # Vintage distribution
            fig2 = px.histogram(
                county_df, 
                x='in_vintage',
                title="Building Vintage Distribution",
                nbins=20
            )
            st.plotly_chart(fig2, use_container_width=True)
        
        # Fuel usage pie charts
        st.subheader("🔥 Fuel Usage Distribution")
        
        col_fuel_1, col_fuel_2 = st.columns(2)
        
        with col_fuel_1:
            # Heating fuel pie chart
            if 'in_heating_fuel' in county_df.columns:
                heating_fuel = county_df['in_heating_fuel'].value_counts()
                fig3 = px.pie(
                    values=heating_fuel.values,
                    names=heating_fuel.index,
                    title="Heating Fuel Distribution"
                )
                st.plotly_chart(fig3, use_container_width=True)
            else:
                st.warning("Heating fuel data not available")
        
        with col_fuel_2:
            # Water heater fuel pie chart
            if 'in_water_heater_fuel' in county_df.columns:
                water_heater_fuel = county_df['in_water_heater_fuel'].value_counts()
                fig4 = px.pie(
                    values=water_heater_fuel.values,
                    names=water_heater_fuel.index,
                    title="Water Heater Fuel Distribution"
                )
                st.plotly_chart(fig4, use_container_width=True)
            else:
                st.warning("Water heater fuel data not available")
        
        # Energy metrics
        st.subheader("🔍 Energy Insights")
        
        col4_1, col4_2, col4_3, col4_4 = st.columns(4)
        
        with col4_1:
            st.write("**Energy Consumption (kWh)**")
            avg_electricity = county_df['out_electricity_total_energy_consumption'].mean()
            st.metric("Avg Electricity", f"{avg_electricity:,.0f}")
        
        with col4_2:
            st.write("**Energy Costs ($)**")
            avg_bill = county_df['out_bills_electricity_usd'].mean()
            st.metric("Avg Electric Bill", f"${avg_bill:.0f}")
        
        with col4_3:
            st.write("**Energy Burden (%)**")
            avg_burden = county_df['out_energy_burden_percentage'].mean()
            st.metric("Avg Energy Burden", f"{avg_burden:.1f}%")
        
        with col4_4:
            st.write("**Primary Heating Fuel**")
            if 'in_heating_fuel' in county_df.columns:
                primary_heating = county_df['in_heating_fuel'].mode().iloc[0] if len(county_df['in_heating_fuel'].mode()) > 0 else "Unknown"
                st.metric("Most Common", primary_heating)
            else:
                st.metric("Most Common", "N/A")
        
        # Data table with pagination
        st.subheader(f"📋 Raw Data Sample - {selected_county}, {display_state}")
        display_columns = ['in_geometry_building_type_recs', 'in_vintage', 'in_geometry_floor_area', 
                          'in_heating_fuel', 'in_water_heater_fuel', 'out_electricity_total_energy_consumption', 
                          'out_bills_electricity_usd', 'out_energy_burden_percentage']
        
        # Filter to only show columns that exist
        available_columns = [col for col in display_columns if col in county_df.columns]
        
        # Pagination controls
        rows_per_page = 20
        total_rows = len(county_df)
        total_pages = (total_rows + rows_per_page - 1) // rows_per_page
        
        # Initialize session state for pagination
        if 'data_page' not in st.session_state:
            st.session_state.data_page = 1
        
        col_pag_1, col_pag_2, col_pag_3 = st.columns([1, 2, 1])
        
        with col_pag_1:
            if total_pages > 1:
                page = st.selectbox(
                    f"Page (1-{total_pages})",
                    options=range(1, total_pages + 1),
                    index=st.session_state.data_page - 1,
                    key="page_selector"
                )
                # Update session state when page changes
                if page != st.session_state.data_page:
                    st.session_state.data_page = page
            else:
                page = 1
                st.session_state.data_page = 1
        
        with col_pag_2:
            st.write(f"Showing page {page} of {total_pages} ({total_rows:,} total buildings)")
        
        with col_pag_3:
            if total_pages > 1:
                col_prev, col_next = st.columns(2)
                with col_prev:
                    if st.button("← Previous", disabled=(page == 1), key="prev_page"):
                        st.session_state.data_page = max(1, page - 1)
                        st.rerun()
                with col_next:
                    if st.button("Next →", disabled=(page == total_pages), key="next_page"):
                        st.session_state.data_page = min(total_pages, page + 1)
                        st.rerun()
        
        # Calculate start and end indices for current page
        start_idx = (page - 1) * rows_per_page
        end_idx = min(start_idx + rows_per_page, total_rows)
        
        # Display the current page of data
        st.dataframe(
            county_df.iloc[start_idx:end_idx][available_columns],
            use_container_width=True
        )
    else:
        st.warning("No data available for selected county")

if __name__ == "__main__":
    main() 