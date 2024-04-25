import os
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import nltk 
nltk.download('vader_lexicon')

from nltk.sentiment.vader import SentimentIntensityAnalyzer

days_data = {}
def analyze_sentiment(news_dir, output_dir, symbols):
    analyzer = SentimentIntensityAnalyzer()

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=29)

    for symbol in symbols:
        curr_data = 0
        sentiment_df = pd.DataFrame(columns=['Date', 'Compound Score'])
    
        news_file = os.path.join(news_dir, f"{symbol}_news.csv")
        if os.path.exists(news_file):
            df = pd.read_csv(news_file, parse_dates=['publishedAt'])
            df['publishedAt'] = pd.to_datetime(df['publishedAt'], utc=True).dt.date

            df = df[(df['publishedAt'] >= start_date) & (df['publishedAt'] <= end_date)]

            for i in range(30): 
                currentDate = end_date - timedelta(days=i)
                articles = df[df['publishedAt'] == currentDate]

                
                if not articles.empty:
                    curr_data += 1
                    sentiment_score = sum(analyzer.polarity_scores(article)['compound'] for article in articles['content']) / len(articles)
                else:
                    sentiment_score = 0.0001 

                sentiment_df = pd.concat([sentiment_df, pd.DataFrame({'Date': currentDate.strftime('%Y-%m-%d'), 'Compound Score': [sentiment_score]})], ignore_index=True)

            output_file = os.path.join(output_dir, f"{symbol}_sentiment_data.csv")
            sentiment_df = sentiment_df.fillna(0)
            sentiment_df.to_csv(output_file, index=False)
            days_data[symbol] = curr_data
        else:
            print(f"No news data found for {symbol}.")

def plot_sentiments_count():
    symbols = list(days_data.keys())
    counts = list(days_data.values())
    
    plt.figure(figsize=(10, 6))
    plt.bar(symbols, counts, color='skyblue')
    plt.xlabel('Stock Symbol')
    plt.ylabel('Number of Days with data')
    plt.title('Number of Days with Sentiment Data')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt_file = f'{output_dir}/sentiment_counts_plot.png'
    plt.savefig(plt_file)

home_dir = "/home/yunusali/airflow/"
news_dir = home_dir + "news"
output_dir = home_dir + "sentiments"
symbols = ['AAPL', 'META', 'AMZN', 'NFLX', 'GOOGL', 'BTC-USD', 'ETH-USD', 'BNB-USD', 'DOGE-USD', 'SOL-USD']

if __name__ == "__main__":
    analyze_sentiment(news_dir, output_dir, symbols)
    plot_sentiments_count()
