import os
import pandas as pd
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from datetime import datetime, timedelta
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler

symbols = ['META', 'AAPL', 'AMZN', 'NFLX', 'GOOGL','BTC-USD', 'ETH-USD', 'BNB-USD', 'DOGE-USD', 'SOL-USD']
# Function to calculate Simple Moving Average
def calculate_sma():
    for symbol in symbols:
        try:
            # Load stock price data
            stock_data = pd.read_csv(f'{home_dir}stocks/{symbol}_stock_data.csv')
            stock_data.sort_values(by='Date', inplace=True)
            stock_data['SMA'] = stock_data['Close'].rolling(window=50).mean()
            stock_data.dropna(inplace=True)
            stock_data.to_csv(f'{home_dir}sma/{symbol}_sma_results.csv', index=False)

        except Exception as e:
            print(f"Error calculating SMA: {e}")



def perform_lagged_sentiment_analysis():
    for symbol in symbols:
        try:
            sentiment_data = pd.read_csv(f'{home_dir}sentiments/{symbol}_sentiment_data.csv')
            sentiment_data['Compound Score'] = pd.to_numeric(sentiment_data['Compound Score'], errors='coerce')  # Convert 'Compound Score' to numeric
            num_lags = 1 

            for i in range(1, num_lags + 1):
                sentiment_data[f'Compound Score Lag {i}'] = sentiment_data['Compound Score'].shift(i)

            sentiment_data.dropna(inplace=True)
            sentiment_data.to_csv(f'{home_dir}lagged_sentiments/{symbol}_lagged_sentiment_data.csv', index=False)

        except Exception as e:
            print(f"Error performing lagged sentiment analysis: {e}")




def plot_technical_analysis():
    for symbol in symbols:
        try:
            # Load stock price data
            stock_data = pd.read_csv(f'{home_dir}stocks/{symbol}_stock_data.csv', parse_dates=['Date'])
            sma_data = pd.read_csv(f'{home_dir}sma/{symbol}_sma_results.csv', parse_dates=['Date'])
            lagged_sentiment_data = pd.read_csv(f'{home_dir}lagged_sentiments/{symbol}_lagged_sentiment_data.csv', parse_dates=['Date'])

            # Filter stock data and SMA data for the past 29 days
            last_29_days_stock_data = stock_data.tail(29)
            last_29_days_sma_data = sma_data.tail(29)

            # Merge datasets on date
            merged_data = pd.merge(last_29_days_stock_data, last_29_days_sma_data, on='Date', how='inner')
            merged_data = pd.merge(merged_data, lagged_sentiment_data, on='Date', how='inner')

            # Plotting
            plt.figure(figsize=(10, 6))
            
            # Plot closing prices and SMA
            plt.plot(merged_data['Date'], merged_data['Close_x'], label='Closing Price', color='blue')
            plt.plot(merged_data['Date'], merged_data['SMA'], label='Simple Moving Average', color='orange')
            
            # Create a secondary y-axis for compound score
            ax1 = plt.gca()
            ax2 = ax1.twinx()
            
            # Plot lagged sentiment on secondary y-axis
            ax2.plot(merged_data['Date'], merged_data['Compound Score Lag 1'], label='Lagged Sentiment', color='green')
            ax2.set_ylabel('Compound Score')  # Set label for secondary y-axis
            
            ax1.legend(loc='upper left')
            ax2.legend(loc='upper right')
            # Add labels and title
            plt.xlabel('Date')
            plt.ylabel('Value')
            plt.title(f'Stock Analysis for {symbol}')
            plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
            plt.legend()

            # Save plot as image
            plt.savefig(f'{home_dir}stock_analysis_plots/{symbol}_stock_analysis_chart.png')
        
        except Exception as e:
            print(f"Error performing stock analysis: {e}")




# Function to create PDF report
def create_pdf_report():
    try:
        # Initialize PDF canvas
        c = canvas.Canvas(home_dir + "stock_analysis_plots/technical_analysis_report.pdf", pagesize=letter)
        width, height = letter
        image_height = 400
        margin = 50
        current_height = height - margin

        # Add plots to PDF
        for symbol in symbols:
            plt_file = f'{home_dir}stock_analysis_plots/{symbol}_stock_analysis_chart.png'  # Path to your plot file
            c.drawImage(plt_file, margin, current_height - image_height, width=500, height=image_height)
            current_height -= (image_height + margin)

            # Add symbol as a title
            c.drawString(margin, current_height + 20, f"Stock Symbol: {symbol}")

            if current_height < (margin * 2 + image_height):
                c.showPage()
                current_height = height - margin
        
        c.drawImage('/home/yunusali/airflow/stock_analysis_plots/correlation_analysis.png', margin, current_height - image_height, width=500, height=image_height)
        current_height -= (image_height + margin)
        if current_height < (margin * 2 + image_height):
            c.showPage()
            current_height = height - margin
        c.drawImage('/home/yunusali/airflow/stock_analysis_plots/kmeans_analysis.png', margin, current_height - image_height, width=500, height=image_height)
        current_height -= (image_height + margin)
        c.save()
    except Exception as e:
        print(f"Error saving PDF report: {e}")


def correlation_matrix_analysis():
    # Load sentiment and stock price data for all stocks
    symbols = ['META', 'AAPL', 'AMZN', 'NFLX', 'GOOGL', 'BTC-USD', 'ETH-USD', 'BNB-USD', 'DOGE-USD', 'SOL-USD']
    correlations = []

    # Load sentiment and stock price data for each stock
    for symbol in symbols:
        sentiment_data = pd.read_csv(f'{home_dir}lagged_sentiments/{symbol}_lagged_sentiment_data.csv', parse_dates=['Date'])
        stock_price_data = pd.read_csv(f'{home_dir}stocks/{symbol}_stock_data.csv', parse_dates=['Date'])
        sentiment_data['Compound Score'] = pd.to_numeric(sentiment_data['Compound Score'], errors='coerce')  # Convert 'Compound Score' to numeric
        
        # Filter sentiment data to match stock price dates
        sentiment_data = sentiment_data[sentiment_data['Date'].isin(stock_price_data['Date'])]
        
        # Calculate correlation between 'Compound Score' and 'Close'
        correlation = sentiment_data['Compound Score'].corr(stock_price_data['Close'])
        
        # Append symbol and correlation to correlations list
        correlations.append({'Symbol': symbol, 'Correlation': correlation})

    # Create correlation dataframe
    correlation_df = pd.DataFrame(correlations)
    # Create correlation dataframe
    correlation_df = pd.DataFrame(correlations)
    correlation_matrix = correlation_df.set_index('Symbol')['Correlation']
     # Plot correlation matrix
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation_matrix.to_frame(), annot=True, cmap='coolwarm', fmt=".2f", linewidths=0.5, linecolor='black')
    plt.title('Correlation Matrix between Sentiment Analysis and Stock Prices')
    plt.xlabel('Stock Symbol')
    plt.ylabel('Lagged Sentiment Analysis')
    plt.savefig('/home/yunusali/airflow/stock_analysis_plots/correlation_analysis.png')
    plt.close()


def performKMeans():
    # Load the stock and sentiment data
    symbols = ['META', 'AAPL', 'AMZN', 'NFLX', 'GOOGL', 'BTC-USD', 'ETH-USD', 'BNB-USD', 'DOGE-USD', 'SOL-USD']
    data = []

    for symbol in symbols:
        stock_data = pd.read_csv(f'{home_dir}stocks/{symbol}_stock_data.csv', parse_dates=['Date'])
        sentiment_data = pd.read_csv(f'{home_dir}lagged_sentiments/{symbol}_lagged_sentiment_data.csv', parse_dates=['Date'])

        # Filter stock data to include only dates present in sentiment data
        stock_data_filtered = stock_data[stock_data['Date'].isin(sentiment_data['Date'])]

        # Aggregate the data for each symbol
        aggregated_data = {
            'Symbol': symbol,
            'Average Close': stock_data_filtered['Close'].mean(),
            'Average Compound Score': sentiment_data['Compound Score Lag 1'].mean()
        }
        data.append(aggregated_data)

    # Create DataFrame from aggregated data
    aggregated_df = pd.DataFrame(data)

    # Select relevant features for clustering
    features = aggregated_df[['Average Close', 'Average Compound Score']]

    # Scale the features
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_features = scaler.fit_transform(features)
    aggregated_df[['Average Close', 'Average Compound Score']] = scaled_features

    # Apply K-means algorithm
    kmeans = KMeans(n_clusters=3, random_state=42)
    clusters = kmeans.fit_predict(scaled_features)

    # Add cluster labels to the DataFrame
    aggregated_df['Cluster'] = clusters

    # Visualize the clusters
    plt.figure(figsize=(10, 6))
    plt.scatter(aggregated_df['Average Close'], aggregated_df['Average Compound Score'], c=aggregated_df['Cluster'], cmap='viridis')
    plt.xlabel('Average Close Price')
    plt.ylabel('Average Compound Score')
    plt.title('K-means Clustering of Stock Symbols')
    plt.colorbar(label='Cluster')
    # Annotate each point with the stock symbol
    for i, symbol in enumerate(aggregated_df['Symbol']):
        plt.annotate(symbol, (aggregated_df['Average Close'][i], aggregated_df['Average Compound Score'][i]))

    plt.grid(True)
    plt.savefig(home_dir + "stock_analysis_plots/kmeans_analysis.png")

home_dir = "/home/yunusali/airflow/"
if __name__ == "__main__":
    calculate_sma()
    perform_lagged_sentiment_analysis()
    plot_technical_analysis()
    correlation_matrix_analysis()
    performKMeans()
    create_pdf_report()