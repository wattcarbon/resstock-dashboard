# ResStock Dashboard

An interactive Streamlit dashboard for exploring building energy data from the ResStock baseline dataset.

## Features

- **County Selection**: Browse and select from 1,842 counties across the US
- **Building Statistics**: View building counts, types, vintage distribution, and more
- **Energy Insights**: Explore energy consumption, costs, and energy burden metrics
- **Interactive Visualizations**: Pie charts and histograms for key building characteristics

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

### Option 1: Interactive Map Dashboard (Recommended)
1. Convert the parquet file to SQLite for better performance:
   ```bash
   python convert_to_sqlite.py
   ```
2. Convert shapefiles to GeoJSON for mapping:
   ```bash
   python convert_shapefiles.py
   ```
3. Run the interactive map dashboard:
   ```bash
   streamlit run app_map.py
   ```

### Option 2: SQLite Database Dashboard
1. Convert the parquet file to SQLite for better performance:
   ```bash
   python convert_to_sqlite.py
   ```
2. Run the SQLite-based dashboard:
   ```bash
   streamlit run app_sqlite.py
   ```

### Option 3: Direct Parquet (Slower)
1. Make sure the `baseline.parquet` file is in the same directory as `app.py`
2. Run the parquet-based dashboard:
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

1. **County Selection**: Dropdown to select any county in the dataset
2. **Key Metrics**: Building counts, weighted counts, and average weights
3. **Building Statistics**: Interactive charts showing:
   - Building type distribution
   - Vintage distribution
   - Floor area distribution
   - Heating fuel types
4. **Energy Insights**: Average electricity consumption, costs, and energy burden
5. **Raw Data**: Sample of the underlying data for the selected county

## Future Enhancements

- Geographic mapping with county boundaries
- Comparative analysis between counties
- Time series analysis
- Energy efficiency upgrade scenarios
- Export functionality for selected data 

## Download Counties

```
curl -o counties.zip "https://www2.census.gov/geo/tiger/TIGER2022/COUNTY/tl_2022_us_county.zip"
unzip counties.zip
```