import asyncio
import csv
from datetime import datetime, timedelta
from deriv_api import DerivAPI
import time

# Replace with your actual API token
API_TOKEN = '9Q9EpVPJNqqNwdj'

async def get_historical_data(api, symbol, timeframe):
    """
    Pulls historical data for a given symbol and timeframe.
    """
    # Calculate start and end times based on user input
    # It's better practice to use integer division for years
    # but a year has a variable number of days so a timedelta of 365 is a close approximation.
    now = datetime.utcnow()
    end_time = int(now.timestamp())

    unit = timeframe[-1].lower()
    value = int(timeframe[:-1])

    if unit == 'y':
        start_time = int((now - timedelta(days=value * 365)).timestamp())
    elif unit == 'm':
        start_time = int((now - timedelta(days=value * 30)).timestamp())
    else:
        print("Invalid timeframe format. Please use '5y' or '5m'.")
        return

    print(f"Fetching data for {symbol} from {datetime.utcfromtimestamp(start_time)} to {datetime.utcfromtimestamp(end_time)}...")

    try:
        # The API call to get historical data
        response = await api.ticks_history(
            ticks_history=symbol,
            end='latest',
            start=start_time,
            style='candles',
            granularity=60 # 60 seconds (1-minute candles)
        )

        if 'history' in response:
            return response['history']
        else:
            print("Failed to retrieve data:", response.get('error', {}).get('message'))
            return None
    except Exception as e:
        print(f"An error occurred while fetching data: {e}")
        return None

async def main():
    """
    Main function to run the script.
    """
    # Connect to the Deriv API
    api = DerivAPI(endpoint='wss://ws.derivws.com/websockets/v3', app_id=1089)
    # The fix is here: pass the token as a positional argument
    await api.authorize(API_TOKEN)

    symbol = '1HZ100V' # This is the symbol for Boom 1000 Index

    # Prompt user for timeframe
    timeframe = input("Enter the timeframe (e.g., '5y' for 5 years, '5m' for 5 months): ")

    # Pull data
    data = await get_historical_data(api, symbol, timeframe)

    if data:
        print(f"Successfully retrieved {len(data['times'])} records.")

        # Prepare data for CSV
        candles = []
        for i in range(len(data['times'])):
            candles.append({
                'time': datetime.fromtimestamp(data['times'][i]),
                'open': data['open'][i],
                'high': data['high'][i],
                'low': data['low'][i],
                'close': data['close'][i]
            })

        # Define CSV file name and headers
        file_name = f'Boom_1000_{timeframe}.csv'
        fieldnames = ['time', 'open', 'high', 'low', 'close']

        # Write data to CSV
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(candles)

        print(f"Data saved to {file_name}")

    # Close the API connection
    await api.disconnect()

if __name__ == "__main__":
    asyncio.run(main())