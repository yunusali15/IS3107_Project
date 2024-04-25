import os
import yfinance as yf

stocks_dir = "/home/yunusali/airflow/stocks/"

def fetch_faang_stock_data():
    try:
        # Define FAANG stock symbols
        symbols = ['META', 'AAPL', 'AMZN', 'NFLX', 'GOOGL','BTC-USD', 'ETH-USD', 'BNB-USD', 'DOGE-USD', 'SOL-USD']

        for symbol in symbols:
            # Fetch stock data
            stock_data = yf.download(symbol, period="200d", interval="1d")

            # Save stock data to CSV file
            csv_filename = f'{stocks_dir}{symbol}_stock_data.csv'
            stock_data.to_csv(csv_filename)
            if not os.path.exists(csv_filename):
                raise Exception(f'Error: Failed to save stock data for {symbol}')
            
    except Exception as e:
        raise Exception(f"An error occurred: {e}")

if __name__ == "__main__":
    fetch_faang_stock_data()
