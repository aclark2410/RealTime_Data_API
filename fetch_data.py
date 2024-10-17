import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import databento as db

# Function to create price bins with a specified step size
def create_price_bins(df, step=0.05):
    """Creates price bins with specified step size."""
    df['price_bins'] = np.floor(df['close'] / step) * step
    return df

# Function to calculate TPO blocks for a single year
def calculate_tpo_blocks(df, time_delta_minutes=30):
    """Calculates TPO blocks for each price level and returns a TPO dictionary."""
    df.index = pd.to_datetime(df.index)  # Ensure the index is in datetime format
    df = df.dropna(subset=['close'])  # Remove rows with missing close prices
    
    # Initialize the TPO dictionary and timestamp tracker
    tpo_dict = {}
    last_tpo_timestamp = {}
    time_delta = pd.Timedelta(minutes=time_delta_minutes)  # Set the time delta (default: 30 minutes)

    # Iterate over each row (1-minute intervals)
    for idx, row in df.iterrows():
        price_bin = row['price_bins']
        current_time = idx  # Timestamp (from df.index)
        
        # Check if this price level has been seen before
        if price_bin not in last_tpo_timestamp:
            tpo_dict[price_bin] = 1  # First occurrence of this price level
            last_tpo_timestamp[price_bin] = current_time
        else:
            # Increment the TPO count if enough time (30 minutes) has passed
            if current_time - last_tpo_timestamp[price_bin] >= time_delta:
                tpo_dict[price_bin] += 1
                last_tpo_timestamp[price_bin] = current_time  # Update the last timestamp

    return tpo_dict

# Function to create and plot TPO histograms for multiple years side by side
def plot_tpo_profiles_side_by_side(tpo_dicts, years):
    """Plots TPO profiles for multiple years side by side."""
    # Get all unique price levels from all years (sorted in descending order)
    all_prices = sorted(set(price for tpo_dict in tpo_dicts for price in tpo_dict.keys()), reverse=True)

    # Create a matrix to store TPO counts for each price level and year
    tpo_matrix = np.zeros((len(all_prices), len(tpo_dicts)))

    # Populate the matrix with TPO counts
    for col, tpo_dict in enumerate(tpo_dicts):
        for price, tpo_count in tpo_dict.items():
            row = all_prices.index(price)
            tpo_matrix[row, col] = tpo_count

    # Plot the TPO histograms side by side
    plt.figure(figsize=(12, 8))
    offset = 0
    for col in range(len(tpo_dicts)):
        plt.step(tpo_matrix[:, col] + offset, all_prices, where='mid', label=f'TPO {years[col]}', marker='o')
        offset += 10  # Shift each year's plot to the right

    # Customize the plot
    plt.title('TPO Market Profile Comparison by Year')
    plt.xlabel('Number of TPO Blocks (Offset for Each Year)')
    plt.ylabel('Price Level')
    plt.grid(True, axis='x')
    plt.legend()
    plt.tight_layout()
    plt.show()

# Function to get data from the API for a specific year
def get_data_for_year(start, end, dataset="GLBX.MDP3", symbol="CLX4"):
    """Retrieves data from the Databento API for a specific time range."""
    client = db.Historical(key="db-HsLUpjXU6TLxiHeEATVeeyWYk5mwe")  # Initialize the API client with your API key
    data = client.timeseries.get_range(
        dataset=dataset,
        symbols=symbol,
        schema="ohlcv-1m",
        start=start,
        end=end
    )
    df = pd.DataFrame(data)  # Convert the data to a DataFrame
    
    # Print the columns to inspect the structure of the data returned by the API
    print(df.columns)
    
    return df

# Main function to run the process for multiple years
def process_years_data(years, dataset, calendar_years):
    """Processes multiple years of data and generates side by side TPO profiles."""
    tpo_dicts = []

    # Use zip to iterate over both years and calendar_years
    for year, calendar_year in zip(years, calendar_years):
        start = f'{year}-01-01'
        end = f'{year}-12-31'
        symbol = f'CLX{calendar_year}'  # Construct the symbol using the calendar year

        # Step 1: Retrieve the data for this year from the API
        df = get_data_for_year(start, end, dataset=dataset, symbol=symbol)
        
        # Step 2: Apply price binning
        df = create_price_bins(df, step=0.05)
        
        # Step 3: Calculate TPO blocks for this year
        tpo_dict = calculate_tpo_blocks(df)
        
        # Store the TPO dictionary
        tpo_dicts.append(tpo_dict)

    # Step 4: Plot the TPO profiles for all years
    plot_tpo_profiles_side_by_side(tpo_dicts, years)

# Example usage:
years = ['2020', '2021', '2022']  # List of years to process
calendar_years = ['0', '1', '2']  # Calendar year suffixes for the symbol

# Process and plot TPO profiles for multiple years
process_years_data(years, "GLBX.MDP3", calendar_years)
