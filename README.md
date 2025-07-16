# ResStock Dashboard

An interactive Streamlit dashboard for exploring building energy data from the ResStock baseline dataset with side-by-side county comparison.

## Features

- **Side-by-Side County Comparison**: Compare two counties simultaneously with separate dashboards
- **Interactive Maps**: County-level choropleth maps showing building density
- **Building Statistics**: View building counts, types, vintage distribution, and more
- **Energy Insights**: Explore energy consumption, costs, and energy burden metrics
- **Interactive Visualizations**: Pie charts and bar charts for key building characteristics
- **Comparison Metrics**: Direct comparison of key metrics between selected counties

## Installation

1. Ensure you have Python 3.8+ installed
2. Activate your virtual environment (if using one):
   ```bash
   source .venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Setup and Run Dashboard
1. Convert the parquet file to SQLite for better performance:
   ```bash
   python convert_to_sqlite.py
   ```
2. Run the comparison dashboard:
   ```bash
   streamlit run app.py
   ```
3. Open your browser and navigate to the URL shown in the terminal (typically `http://localhost:8501`)

## Data Structure

The dashboard uses the ResStock baseline dataset which contains:
- **549,718 buildings** across **1,842 counties**
- Building characteristics (type, vintage, floor area, etc.)
- Energy consumption data
- Cost and energy burden metrics

## Dashboard Sections

### County 1 & County 2 Dashboards
Each county dashboard includes:
1. **Interactive County Map**: Choropleth map showing building count by county
2. **County Selection**: Dropdown to select any county in the dataset
3. **Key Metrics**: Building counts and weighted counts
4. **Distribution Charts**: Interactive charts showing:
   - Building type distribution (pie chart)
   - Vintage distribution (bar chart, sorted by NREL vintage order)
   - Heating fuel distribution (pie chart)
   - Water heater fuel distribution (pie chart)
5. **Energy Insights**: Average electricity consumption, costs, and energy burden
6. **Energy Characteristics**: Primary heating fuel information
7. **County Summary Table**: Comprehensive data table for the selected county

### Side-by-Side Comparison
- **Building Count Comparison**: Direct comparison with delta values
- **Average Floor Area Comparison**: Size differences between counties
- **Average Electricity Comparison**: Energy consumption differences
- **Average Energy Burden Comparison**: Energy cost burden differences

## File Structure

- `app.py` - Main dashboard application
- `convert_to_sqlite.py` - Converts parquet data to SQLite database
- `counties.geojson` - County boundary data for mapping
- `resstock.db` - SQLite database with county summaries
- `baseline.parquet` - Original ResStock dataset
- `state_lookup.csv` - State name/abbreviation mapping
- `county_lookup.csv` - County information lookup table

## Future Enhancements

- Additional comparison metrics
- Time series analysis
- Energy efficiency upgrade scenarios
- Export functionality for selected data
- More detailed building characteristic comparisons