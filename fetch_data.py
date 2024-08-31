import pandas as pd
from collections import defaultdict
from datetime import timedelta, datetime
import matplotlib.pyplot as plt
import numpy as np
import mplfinance as mpf

# Load the data from a CSV file
df = pd.read_csv('sp500_5years.csv', index_col='Date', parse_dates=True)

# Define the range of days you want to process
start_date = datetime(2018, 1, 2)
end_date = datetime(2022, 12, 30)  # Example: Process 5 days of data

# Filter the DataFrame to only include data within the specified date range
df = df[(df.index >= start_date) & (df.index <= end_date)]

# Loop through each day and process data
current_date = start_date
while current_date <= end_date:
    next_date = current_date + timedelta(days=1)
    
    # Filter data for the current day
    df_day = df[(df.index >= current_date) & (df.index < next_date)]
    
    # Ensure the DataFrame is sorted by the index (timestamp)
    df_day.sort_index(inplace=True)
    
    # Initialize the TPO dictionary
    tpo_dict = defaultdict(int)
    
    # Track the current 30-minute period start
    current_period_start = df_day.index[0]
    time_frame = timedelta(minutes=30)
    
    # Initialize a set to track prices within the current time frame
    current_time_frame_prices = set()
    
    # Iterate through the DataFrame to calculate TPO blocks
    for timestamp, row in df_day.iterrows():
        price = row['Close']  # Use the 'Close' price for TPO
        
        # Check if we are still within the same 30-minute time frame
        if timestamp >= current_period_start + time_frame:
            # Reset for the new time frame
            current_period_start += time_frame
            current_time_frame_prices.clear()
        
        # If the price hasn't been added to the TPO block in this time frame, update the TPO dictionary
        if price not in current_time_frame_prices:
            tpo_dict[price] += 1
            current_time_frame_prices.add(price)
    
    # Get the full range of prices traded during this period
    min_price = df_day['Close'].min()
    max_price = df_day['Close'].max()
    price_range = np.arange(min_price, max_price + 0.25, 0.25)  # Assuming tick size is 0.25
    
    # Create a TPO vector with 0s for prices that were not traded
    tpo_vector = np.array([tpo_dict[price] if price in tpo_dict else 0 for price in price_range])
    
    # Resample the original data to 30-minute intervals for candlestick plotting
    ohlc_dict = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum'
    }
    df_30min = df_day.resample('30min').agg(ohlc_dict)
    df_30min.dropna(inplace=True)
    
    # Plot TPO Profile and Candlestick Chart for the Current Day
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 8))
    
    # Plot the TPO histogram on the first subplot
    ax1.barh(price_range, tpo_vector, color='blue')
    ax1.set_xlabel('TPO Blocks')
    ax1.set_ylabel('Price Levels')
    ax1.set_title(f'TPO Chart - {current_date.strftime("%Y-%m-%d")}')
    ax1.grid(True)
    
    # Plot the 30-minute candlestick chart on the second subplot (without volume bars)
    mpf.plot(
        df_30min,
        type='candle',
        ax=ax2,  # Candlestick plot on the price axis
        style='charles',
        ylabel='Price',
        show_nontrading=False,
        xrotation=15  # Adjust to your preference
    )
    
    # Manually set the title for the candlestick plot
    ax2.set_title(f'Candlestick Chart - {current_date.strftime("%Y-%m-%d")}')
    
    plt.tight_layout()
    plt.show()
    
    # Move to the next day
    current_date = next_date
