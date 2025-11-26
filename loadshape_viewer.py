#!/usr/bin/env python3
from datetime import timedelta
from eemeter.eemeter import HourlyBaselineData
from eemeter.eemeter import HourlyReportingData

import ciso8601
import streamlit as st
import pandas as pd
import sqlite3
import plotly.graph_objects as go
import json
import os
import requests
import random
from io import BytesIO, StringIO

from hourly import create_and_fit_hourly_model

DB_LOCAL_PATH = "resstock_building_lookup.db"
DB_URL = "https://wattcarbon-sandbox-resstock.s3.amazonaws.com/resstock.db"

@st.cache_data
def download_db():
    if not os.path.exists(DB_LOCAL_PATH):
        with st.spinner("Downloading database..."):
            r = requests.get(DB_URL)
            r.raise_for_status()
            with open(DB_LOCAL_PATH, "wb") as f:
                f.write(r.content)
    return DB_LOCAL_PATH

db_path = download_db()

# Set page config
st.set_page_config(
    page_title="ResStock Demand Response Evaluation",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title
st.title("📊 ResStock Demand Response Evaluation")
st.markdown("View random loadshapes from NREL ResStock by selecting a state, upgrade, and building type")

@st.cache_data
def load_upgrades_lookup():
    """Load upgrades lookup from JSON file"""
    try:
        with open('upgrades_lookup.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error("❌ upgrades_lookup.json file not found!")
        return {}
    except Exception as e:
        st.error(f"❌ Error loading upgrades_lookup.json: {e}")
        return {}

@st.cache_data
def get_available_states():
    """Get list of available states from building_lookup table"""
    try:
        conn = sqlite3.connect(db_path)
        query = "SELECT DISTINCT state FROM building_lookup ORDER BY state"
        states = pd.read_sql_query(query, conn)
        conn.close()
        return states['state'].tolist()
    except Exception as e:
        st.error(f"❌ Error fetching available states: {e}")
        return []

@st.cache_data
def get_available_building_types():
    """Get list of available building types from building_lookup table"""
    try:
        conn = sqlite3.connect(db_path)
        query = "SELECT DISTINCT building_type FROM building_lookup ORDER BY building_type"
        building_types = pd.read_sql_query(query, conn)
        conn.close()
        return building_types['building_type'].tolist()
    except Exception as e:
        st.error(f"❌ Error fetching available building types: {e}")
        return []

def get_random_building_id(state, building_type):
    """Get a random building ID from building_lookup table matching state and building type"""
    conn = sqlite3.connect(db_path)
    query = """
    SELECT bldg_id 
    FROM building_lookup 
    WHERE state = ? AND building_type = ?
    ORDER BY RANDOM()
    LIMIT 1
    """
    result = pd.read_sql_query(query, conn, params=[state, building_type])
    conn.close()
    
    if len(result) == 0:
        raise Exception(f"No building ID found for {state} - {building_type}")
    return result['bldg_id'].iloc[0].astype(str)

def get_building_info(building_id):
    conn = sqlite3.connect(db_path)
    # First get county ID from building_lookup
    query1 = """
    SELECT state, building_type, county 
    FROM building_lookup 
    WHERE bldg_id = ?
    LIMIT 1
    """
    result = pd.read_sql_query(query1, conn, params=[building_id])
    
    if len(result) == 0:
        conn.close()
        raise Exception(f"Building ID {building_id} not found in database")
    
    state = result['state'].iloc[0]
    building_type = result['building_type'].iloc[0]
    county_id = result['county'].iloc[0]
    
    conn.close()
    return state, building_type, county_id

@st.cache_data
def get_weather_data(state, county_id) -> pd.Series:
    base_url = "https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock"
    suffix = f"2024/resstock_amy2018_release_2/weather/state={state}/{county_id}_2018.csv"
    url = f"{base_url}/{suffix}"
    
    # Fetch CSV file
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    # Read CSV from response
    df = pd.read_csv(StringIO(response.text))
    resstock_series = df.set_index("date_time")["Dry Bulb Temperature [Â°C]"]
    resstock_series.index = resstock_series.index.map(ciso8601.parse_datetime) - pd.Timedelta(
        hours=1
    )
    resstock_series = resstock_series.resample("h").mean().copy()
    resstock_series = resstock_series.tz_localize("Etc/GMT+4")
    
    return resstock_series

def get_available_upgrades(upgrades_lookup):
    """Get list of available upgrades from upgrades lookup"""
    return [int(k) for k in upgrades_lookup.keys() if k.isdigit()]

def get_loadshape_data(building_id, state, upgrade):
    """Get loadshape data from S3 for a specific building ID, state, and upgrade"""
    # Construct URL
    base_url = "https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock"
    suffix = f"2024/resstock_amy2018_release_2/timeseries_individual_buildings/by_state/upgrade={upgrade}/state={state}/{building_id}-{upgrade}.parquet"
    url = f"{base_url}/{suffix}"
    
    # Fetch parquet file
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    # Read parquet from bytes
    df = pd.read_parquet(BytesIO(response.content))
    
    # Convert timestamp to datetime if it exists
    df = df.set_index(pd.to_datetime(df["timestamp"]))
    df.index = df.index - pd.Timedelta(minutes=15)
    df = df.tz_localize("Etc/GMT+4")
    
    # Find the electricity total energy consumption column
    electricity_col = 'out.electricity.total.energy_consumption'

    return df[electricity_col].resample("h").sum()
    
def make_prediction(weather_data, loadshape_data, selected_date):
    baseline_loadshape_data = loadshape_data[(loadshape_data.index.date>=selected_date-timedelta(days=28))&(loadshape_data.index.date<selected_date)]
    baseline_weather_data = weather_data[(weather_data.index.date>=selected_date-timedelta(days=28))&(weather_data.index.date<selected_date)]

    hourly_baseline_data = HourlyBaselineData.from_series(
        baseline_loadshape_data,
        baseline_weather_data,
        is_electricity_data=True,
    )

    hourly_model = create_and_fit_hourly_model(hourly_baseline_data, "single", include_occupancy=False)

    hourly_reporting_data = HourlyReportingData.from_series(
        loadshape_data[loadshape_data.index.date==selected_date],
        weather_data[weather_data.index.date==selected_date],
        is_electricity_data=True,
    )

    return hourly_model.predict(reporting_data=hourly_reporting_data)
        

def main():
    # Check if database exists
    try:
        conn = sqlite3.connect(db_path)
        conn.close()
    except Exception as e:
        st.error(f"❌ Database Error: {e}")
        st.error("❌ SQLite database not found! Please run `python convert_to_sqlite.py` first.")
        st.stop()
    
    # Load upgrades lookup
    upgrades_lookup = load_upgrades_lookup()
    if not upgrades_lookup:
        st.error("❌ Could not load upgrades lookup. Please check upgrades_lookup.json file.")
        st.stop()
    
    # Get available options
    available_states = get_available_states()
    available_building_types = get_available_building_types()
    available_upgrades = get_available_upgrades(upgrades_lookup)
    
    if not available_states or not available_building_types:
        st.warning("⚠️ No building lookup data found in database. Please run `python convert_to_sqlite.py building_lookup` first.")
        st.stop()
    
    if not available_upgrades:
        st.warning("⚠️ No upgrades found in upgrades_lookup.json file.")
        st.stop()
    
    # Read URL parameters
    query_params = st.query_params
    url_building_id = query_params.get("building_id", None)
    url_upgrade = query_params.get("upgrade", None)
    url_date = query_params.get("date", None)
    url_start_hour = query_params.get("start_hour", None)
    url_end_hour = query_params.get("end_hour", None)
    
    # If building_id is in URL, look up state, building_type, and county
    url_state = None
    url_building_type = None
    if url_building_id:
        url_state, url_building_type, county_id = get_building_info(url_building_id)
        if url_state is None or url_building_type is None:
            st.warning(f"⚠️ Building ID {url_building_id} not found in database.")
            url_building_id = None
    
    # Sidebar for filters
    with st.sidebar:
        st.header("🔍 Filters")
        
        # State selector - use URL param if available
        default_state_index = 0
        if url_state and url_state in available_states:
            default_state_index = available_states.index(url_state)
        else:
            default_state_index = available_states.index('CA')
        
        selected_state = st.selectbox(
            "Select State:",
            options=available_states,
            index=default_state_index,
            help="Select the state for the loadshape"
        )
        
        # Upgrade selector - use URL param if available
        upgrade_options = [u for u in available_upgrades if str(u) in upgrades_lookup]
        default_upgrade_index = 0
        if url_upgrade:
            try:
                upgrade_val = int(url_upgrade)
                if upgrade_val in upgrade_options:
                    default_upgrade_index = upgrade_options.index(upgrade_val)
            except ValueError:
                pass
        
        selected_upgrade = st.selectbox(
            "Select Upgrade:",
            options=upgrade_options,
            index=default_upgrade_index,
            format_func=lambda x: f"{x}: {upgrades_lookup[str(x)]}",
            help="Select the upgrade scenario"
        )
        
        # Building type selector - use URL param if available
        default_building_type_index = 0
        if url_building_type and url_building_type in available_building_types:
            default_building_type_index = available_building_types.index(url_building_type)
        
        selected_building_type = st.selectbox(
            "Select Building Type:",
            options=available_building_types,
            index=default_building_type_index,
            help="Select the building type"
        )
        
        # Randomize button
        if st.button("🎲 Randomize Selection", type="primary"):
            # Randomly select from available options
            selected_state = random.choice(available_states)
            selected_upgrade = random.choice(upgrade_options)
            selected_building_type = random.choice(available_building_types)
            # Clear URL params when randomizing
            st.query_params.clear()
            st.rerun()
    
    # Initialize session state for building ID and loadshape data
    if 'current_building_id' not in st.session_state:
        st.session_state.current_building_id = None
    if 'loadshape_series' not in st.session_state:
        st.session_state.loadshape_series = None
    if 'loadshape_hod_data' not in st.session_state:
        st.session_state.loadshape_hod_data = None
    
    # Get building ID - use URL param if available, otherwise get random
    random_bldg_id = url_building_id or get_random_building_id(selected_state, selected_building_type)
    selected_state, selected_building_type, county_id = get_building_info(random_bldg_id)
    
    if not random_bldg_id:
        st.warning(f"⚠️ No buildings found for {selected_state} - {selected_building_type}")
        st.info("Try selecting different state or building type.")
        return
    
    # Check if we need to fetch new data
    # Create a cache key based on building ID, state, and upgrade
    cache_key = f"{random_bldg_id}_{selected_state}_{selected_upgrade}"

    # Check if we have cached data for this exact combination
    if ('loadshape_cache_key' not in st.session_state or 
        st.session_state.loadshape_cache_key != cache_key or
        st.session_state.loadshape_series is None):
        
        # Fetch new data
        with st.spinner(f"Fetching loadshape data for building {random_bldg_id}..."):
            loadshape_series = get_loadshape_data(random_bldg_id, selected_state, selected_upgrade)
            
            if loadshape_series is None or len(loadshape_series) == 0:
                st.warning("⚠️ No loadshape data available for the selected combination.")
                st.info("Try selecting different state, upgrade, or building type.")
                return
            
            loadshape_hod_data = loadshape_series.groupby(loadshape_series.index.hour).mean()
            
            # Store in session state
            st.session_state.loadshape_cache_key = cache_key
            st.session_state.loadshape_series = loadshape_series
            st.session_state.loadshape_hod_data = loadshape_hod_data
    else:
        # Use cached data from session state
        loadshape_series = st.session_state.loadshape_series
        loadshape_hod_data = st.session_state.loadshape_hod_data

    with st.spinner(f"Fetching weather data for {county_id}, {selected_state}..."):
        weather_data = get_weather_data(selected_state, county_id)

    
    
    # Main content area
    st.subheader(f"Loadshape for Building {random_bldg_id} - {selected_state} - {upgrades_lookup[str(selected_upgrade)]} - {selected_building_type}")
    
    st.info(f"🏠 **Building ID:** {random_bldg_id}")
    
    # Get date range from the data
    min_date = loadshape_series.index.min().date()
    max_date = loadshape_series.index.max().date()
    
    # Date picker - use URL param if available
    default_date = min_date + pd.Timedelta(days=180)
    if url_date:
        try:
            parsed_date = pd.to_datetime(url_date).date()
            if min_date <= parsed_date <= max_date:
                default_date = parsed_date
        except (ValueError, TypeError):
            pass
    
    st.subheader("📅 Select Date")
    selected_date = st.date_input(
        "Choose a date to view:",
        value=default_date,
        min_value=min_date,
        max_value=max_date,
        help="Select a date to view the loadshape for that specific day and the 28 days before"
    )
    
    # Hour range picker - use URL params if available
    default_start_hour = 17
    default_end_hour = 21
    if url_start_hour:
        try:
            default_start_hour = int(url_start_hour)
            if default_start_hour < 0 or default_start_hour > 24:
                default_start_hour = 17
        except ValueError:
            pass
    if url_end_hour:
        try:
            default_end_hour = int(url_end_hour)
            if default_end_hour < 0 or default_end_hour > 24:
                default_end_hour = 21
        except ValueError:
            pass
    
    st.subheader("⏰ Select Hour Range")
    hour_range = st.slider(
        "Select hour range to highlight:",
        min_value=0,
        max_value=24,
        value=(default_start_hour, default_end_hour),
        step=1,
        help="Select a range of hours to highlight on the selected day chart"
    )
    start_hour, end_hour = hour_range
    
    # Update URL parameters when values change
    new_params = {
        "building_id": random_bldg_id,
        "upgrade": str(selected_upgrade),
        "date": selected_date.isoformat(),
        "start_hour": str(start_hour),
        "end_hour": str(end_hour)
    }
    
    # Only update if params have changed
    current_params = dict(st.query_params)
    if current_params != new_params:
        st.query_params.update(new_params)

    # Make Prediction
    with st.spinner(f"Making prediction for {selected_date}..."):
        prediction = make_prediction(weather_data, loadshape_series, selected_date)
    
    # Convert selected_date to datetime for filtering
    # Get timezone from the series or use UTC
    tz = loadshape_series.index.tz if loadshape_series.index.tz else 'UTC'
    selected_datetime_start = pd.Timestamp(selected_date, tz=tz)

    
    
    # Filter data for 28 days before selected day
    days_28_before_start = selected_datetime_start - pd.Timedelta(days=28)
    days_28_before_data = loadshape_series[
        (loadshape_series.index >= days_28_before_start) & 
        (loadshape_series.index < selected_datetime_start)
    ]
    
    
    # Display metrics
    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    
    with metric_col1:
        st.metric("Total (24h)", f"{loadshape_hod_data.sum():,.2f} kWh")
    
    with metric_col2:
        st.metric("Average (per hour)", f"{loadshape_hod_data.mean():,.2f} kWh")
    
    with metric_col3:
        st.metric("Peak Hour", f"Hour {loadshape_hod_data.idxmax()}")
    
    with metric_col4:
        st.metric("Peak Value", f"{loadshape_hod_data.max():,.2f} kWh")
    
    # Calculate savings for selected hour range (after prediction is made)
    # This will be calculated later when we have predicted and observed
    
    # Create charts section
    st.subheader("📊 Daily Loadshape Charts")
    
    # Filter weather data for 28 days before
    weather_28_days = weather_data[
        (weather_data.index >= days_28_before_start) & 
        (weather_data.index < selected_datetime_start)
    ]
        
    # Temperature chart - combined
    st.subheader("🌡️ Temperature Data")
    
    # Temperature for selected day
    weather_selected_day = weather_data[weather_data.index.date == selected_date]
    weather_selected_hourly = weather_selected_day.groupby(weather_selected_day.index.hour).mean()
    
    # Temperature for 28 days before (average)
    weather_28_hourly = weather_28_days.groupby(weather_28_days.index.hour).mean()
    
    fig_temp = go.Figure()
    
    # Add 28-day average first (so it appears behind)
    fig_temp.add_trace(go.Scatter(
        x=weather_28_hourly.index,
        y=weather_28_hourly.values,
        mode='lines+markers',
        name="28-Day Average",
        line=dict(width=3, color='#2ca02c', shape='hv'),
        marker=dict(size=8, color='#2ca02c'),
        fill='tozeroy',
        fillcolor='rgba(44, 160, 44, 0.1)'
    ))
    
    # Add selected day on top
    fig_temp.add_trace(go.Scatter(
        x=weather_selected_hourly.index,
        y=weather_selected_hourly.values,
        mode='lines+markers',
        name=f"Selected Day: {selected_date}",
        line=dict(width=3, color='#ff7f0e', shape='hv'),
        marker=dict(size=8, color='#ff7f0e')
    ))
    
    fig_temp.update_layout(
        title=f"Temperature Comparison: Selected Day vs 28-Day Average",
        xaxis_title="Hour of Day",
        yaxis_title="Temperature (°C)",
        height=400,
        showlegend=True,
        hovermode='x unified',
        xaxis=dict(
            tickmode='linear',
            tick0=0,
            dtick=1,
            range=[0, 23]
        ),
        template='plotly_white'
    )
    
    st.plotly_chart(fig_temp, use_container_width=True)
        
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        # Chart for selected day - show predicted vs observed
        predicted = prediction.predicted
        observed = prediction.observed
        
        # Calculate savings for selected hour range
        # Get hours from the index
        if hasattr(predicted.index, 'hour'):
            pred_hours = predicted.index.hour
            obs_hours = observed.index.hour if hasattr(observed.index, 'hour') else [i % 24 for i in range(len(observed))]
        else:
            pred_hours = [i % 24 for i in range(len(predicted))]
            obs_hours = [i % 24 for i in range(len(observed))]
        
        # Filter to selected hour range and calculate savings
        pred_hour_series = pd.Series(pred_hours, index=predicted.index)
        obs_hour_series = pd.Series(obs_hours, index=observed.index)
        
        # Get data for selected hours
        selected_pred = predicted[(pred_hour_series >= start_hour) & (pred_hour_series < end_hour)]
        selected_obs = observed[(obs_hour_series >= start_hour) & (obs_hour_series < end_hour)]
        
        # Calculate savings (predicted - observed)
        if len(selected_pred) > 0 and len(selected_obs) > 0:
            # Align by index if they have the same index
            if selected_pred.index.equals(selected_obs.index):
                savings = (selected_pred - selected_obs).sum()
            else:
                # Try to align by matching indices
                common_idx = selected_pred.index.intersection(selected_obs.index)
                if len(common_idx) > 0:
                    savings = (selected_pred.loc[common_idx] - selected_obs.loc[common_idx]).sum()
                else:
                    # Fallback: sum individually and subtract
                    savings = selected_pred.sum() - selected_obs.sum()
        else:
            savings = 0.0
        
        # Get hour of day for x-axis
        hours = pred_hours
        
        fig_selected = go.Figure()
        
        # Add vertical rectangle for hour range if range is selected
        # Use yref="paper" to make it span the full y-axis height
        if start_hour < end_hour:
            fig_selected.add_shape(
                type="rect",
                x0=start_hour,
                y0=0,
                x1=end_hour,
                y1=1,
                xref="x",
                yref="paper",
                fillcolor="rgba(255, 0, 0, 0.2)",
                line=dict(color="rgba(255, 0, 0, 0.5)", width=2),
                layer="below"
            )
        
        # Add observed values
        fig_selected.add_trace(go.Scatter(
            x=hours,
            y=observed.values,
            mode='lines+markers',
            name="Observed",
            line=dict(width=3, color='#1f77b4', shape='hv'),
            marker=dict(size=8, color='#1f77b4', opacity=0.7),
            opacity=0.7,
            fill='tozeroy',
            fillcolor='rgba(31, 119, 180, 0.1)'
        ))
        
        # Add predicted values
        fig_selected.add_trace(go.Scatter(
            x=hours,
            y=predicted.values,
            mode='lines+markers',
            name="Predicted",
            line=dict(width=3, color='#ff7f0e', shape='hv'),
            marker=dict(size=8, color='#ff7f0e', symbol='diamond', opacity=0.7),
            opacity=0.7
        ))
        
        fig_selected.update_layout(
            title=f"Selected Day: {selected_date} - Predicted vs Observed",
            xaxis_title="Hour of Day",
            yaxis_title="Electricity Consumption (kWh)",
            height=400,
            showlegend=True,
            hovermode='x unified',
            xaxis=dict(
                tickmode='linear',
                tick0=0,
                dtick=1,
                range=[0, 23]
            ),
            template='plotly_white'
        )
        
        st.plotly_chart(fig_selected, use_container_width=True)
        
        # Display selected hour range info and prediction accuracy
        if start_hour < end_hour:
            st.caption(f"📌 Highlighted hours: {start_hour}:00 - {end_hour}:00")
        
        # Display savings metric for selected hours
        st.metric(
            f"Savings (Hours {start_hour}-{end_hour})",
            f"{savings:,.2f} kWh",
            help="Difference between predicted and observed energy usage for the selected hour range (positive = predicted higher, negative = observed higher)"
        )
        
        # Calculate and display prediction metrics
        if len(predicted) > 0 and len(observed) > 0:
            mape = (abs(predicted.values - observed.values) / observed.values * 100).mean()
            rmse = ((predicted.values - observed.values) ** 2).mean() ** 0.5
            st.metric("Mean Absolute Percentage Error (MAPE)", f"{mape:.2f}%")
            st.metric("Root Mean Square Error (RMSE)", f"{rmse:.2f} kWh")
    
    with chart_col2:
        # Chart for 28 days before - show baseline used for prediction
        if len(days_28_before_data) > 0:
            # Group by hour and calculate average across all 28 days
            days_28_before_hourly = days_28_before_data.groupby(days_28_before_data.index.hour).mean()
            
            fig_28days = go.Figure()
            
            # Add baseline average
            fig_28days.add_trace(go.Scatter(
                x=days_28_before_hourly.index,
                y=days_28_before_hourly.values,
                mode='lines+markers',
                name="Baseline (28-day avg)",
                line=dict(width=3, color='#2ca02c', shape='hv'),
                marker=dict(size=8, color='#2ca02c'),
                fill='tozeroy',
                fillcolor='rgba(44, 160, 44, 0.1)'
            ))
            
            # Add predicted line for comparison
            if len(predicted) > 0:
                pred_hours = predicted.index.hour if hasattr(predicted.index, 'hour') else range(len(predicted))
                fig_28days.add_trace(go.Scatter(
                    x=pred_hours,
                    y=predicted.values,
                    mode='lines+markers',
                    name="Predicted (selected day)",
                    line=dict(width=3, color='#ff7f0e', shape='hv'),
                    marker=dict(size=8, color='#ff7f0e', symbol='diamond', opacity=0.7),
                    opacity=0.7
                ))
            
            fig_28days.update_layout(
                title=f"Baseline vs Prediction: {days_28_before_start.date()} to {selected_date}",
                xaxis_title="Hour of Day",
                yaxis_title="Electricity Consumption (kWh)",
                height=400,
                showlegend=True,
                hovermode='x unified',
                xaxis=dict(
                    tickmode='linear',
                    tick0=0,
                    dtick=1,
                    range=[0, 23]
                ),
                template='plotly_white'
            )
            
            st.plotly_chart(fig_28days, use_container_width=True)
        else:
            st.info(f"No data available for the 28 days before {selected_date}")
    
    # Temperature vs Energy Usage scatter plot
    st.subheader("🌡️📊 Temperature vs Energy Usage")
    
    # Prepare baseline data (28 days before) - align by timestamp
    baseline_aligned = pd.DataFrame({
        'temperature': weather_28_days,
        'energy': days_28_before_data
    }).dropna()
    
    # Prepare prediction day data - align by timestamp
    prediction_day_aligned = pd.DataFrame({
        'temperature': weather_selected_day,
        'energy': observed
    }).dropna()
    
    # If indices don't match exactly, try to align them
    if len(baseline_aligned) == 0 and len(weather_28_days) > 0 and len(days_28_before_data) > 0:
        # Reindex to align timestamps
        common_index = weather_28_days.index.intersection(days_28_before_data.index)
        if len(common_index) > 0:
            baseline_aligned = pd.DataFrame({
                'temperature': weather_28_days.loc[common_index],
                'energy': days_28_before_data.loc[common_index]
            }).dropna()
    
    if len(prediction_day_aligned) == 0 and len(weather_selected_day) > 0 and len(observed) > 0:
        # Reindex to align timestamps
        common_index = weather_selected_day.index.intersection(observed.index)
        if len(common_index) > 0:
            prediction_day_aligned = pd.DataFrame({
                'temperature': weather_selected_day.loc[common_index],
                'energy': observed.loc[common_index]
            }).dropna()
    
    fig_temp_energy = go.Figure()
    
    # Add baseline scatter (28 days)
    if len(baseline_aligned) > 0:
        fig_temp_energy.add_trace(go.Scatter(
            x=baseline_aligned['temperature'],
            y=baseline_aligned['energy'],
            mode='markers',
            name='Baseline (28 days)',
            marker=dict(
                size=6,
                color='#2ca02c',
                opacity=0.6,
                line=dict(width=1, color='#2ca02c')
            )
        ))
    
    # Add prediction day scatter
    if len(prediction_day_aligned) > 0:
        fig_temp_energy.add_trace(go.Scatter(
            x=prediction_day_aligned['temperature'],
            y=prediction_day_aligned['energy'],
            mode='markers',
            name=f'Prediction Day ({selected_date})',
            marker=dict(
                size=10,
                color='#ff7f0e',
                opacity=0.8,
                line=dict(width=2, color='#ff7f0e')
            )
        ))
    
    fig_temp_energy.update_layout(
        title="Temperature vs Energy Usage: Baseline vs Prediction Day",
        xaxis_title="Temperature (°C)",
        yaxis_title="Energy Usage (kWh)",
        height=500,
        showlegend=True,
        hovermode='closest',
        template='plotly_white'
    )
    
    st.plotly_chart(fig_temp_energy, use_container_width=True)
    
    # Also show the average loadshape chart
    st.subheader("📊 Average Loadshape (All Data)")
    fig_avg = go.Figure()
    
    fig_avg.add_trace(go.Scatter(
        x=loadshape_hod_data.index,
        y=loadshape_hod_data.values,
        mode='lines+markers',
        name="Average Electricity Consumption",
        line=dict(width=3, color='#1f77b4', shape='hv'),
        marker=dict(size=8, color='#1f77b4'),
        fill='tozeroy',
        fillcolor='rgba(31, 119, 180, 0.1)'
    ))
    
    # Update layout
    fig_avg.update_layout(
        title="24-Hour Average Electricity Loadshape (All Available Data)",
        xaxis_title="Hour of Day",
        yaxis_title="Electricity Consumption (kWh)",
        height=500,
        showlegend=True,
        hovermode='x unified',
        xaxis=dict(
            tickmode='linear',
            tick0=0,
            dtick=1,
            range=[0, 23]
        ),
        template='plotly_white'
    )
    
    st.plotly_chart(fig_avg, use_container_width=True)
    
    # Display data tables
    with st.expander("📋 View Raw Data"):
        tab1, tab2, tab3 = st.tabs(["Average (All Data)", "Selected Day", "28 Days Before"])
        
        with tab1:
            display_df = pd.DataFrame({
                'Hour': loadshape_hod_data.index,
                'Electricity Consumption (kWh)': loadshape_hod_data.values
            })
            st.dataframe(display_df, use_container_width=True)
        
        with tab2:
            if len(observed) > 0 and len(predicted) > 0:
                obs_hours = observed.index.hour if hasattr(observed.index, 'hour') else range(len(observed))
                display_df_selected = pd.DataFrame({
                    'Hour': obs_hours,
                    'Observed (kWh)': observed.values,
                    'Predicted (kWh)': predicted.values,
                    'Difference (kWh)': (observed.values - predicted.values)
                })
                st.dataframe(display_df_selected, use_container_width=True)
            else:
                st.info(f"No data available for {selected_date}")
        
        with tab3:
            if len(days_28_before_data) > 0:
                days_28_before_hourly = days_28_before_data.groupby(days_28_before_data.index.hour).mean()
                display_df_28days = pd.DataFrame({
                    'Hour': days_28_before_hourly.index,
                    'Electricity Consumption (kWh)': days_28_before_hourly.values
                })
                st.dataframe(display_df_28days, use_container_width=True)
            else:
                st.info(f"No data available for the 28 days before {selected_date}")
    
    # Information section
    st.divider()
    st.subheader("ℹ️ Information")
    
    info_col1, info_col2 = st.columns(2)
    
    with info_col1:
        st.write("**Selection Details:**")
        st.write(f"- **Building ID:** {random_bldg_id}")
        st.write(f"- **State:** {selected_state}")
        st.write(f"- **Upgrade:** {selected_upgrade} - {upgrades_lookup[str(selected_upgrade)]}")
        st.write(f"- **Building Type:** {selected_building_type}")
    
    with info_col2:
        st.write("**Data Source:**")
        st.write("- NREL ResStock 2018 Release 1.1")
        st.write("- Individual building loadshapes")
        st.write("- Fetched from S3 parquet files")
        st.write("- Total electricity consumption")

if __name__ == "__main__":
    main()

