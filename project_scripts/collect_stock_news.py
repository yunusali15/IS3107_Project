import csv
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import newspaper
from newspaper import Config
import requests
import matplotlib.pyplot as plt

api_key = '94aa0283848049f69ef750b3114102b9'

def get_article_content(article_url):
    try:
        config = Config()
        config.browser_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        
        article = newspaper.Article(article_url, config=config)
        article.download()
        article.parse()
        return article.text
    except Exception as e:
        print(f"An error occurred while fetching article content: {article_url}")
        return ""

def save_news_to_csv(stock_symbol):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    from_date = start_date.strftime('%Y-%m-%d')
    to_date = end_date.strftime('%Y-%m-%d')
    sources = 'bloomberg, business-insider, financial-post, fortune, the-wall-street-journal, cnn, crypto-coins-news, google-news, newsweek, politico'

    url = f'https://newsapi.org/v2/everything?sources={sources}&apiKey={api_key}&q={stocks[stock_symbol]}&from={from_date}&to={to_date}&sortBy=popularity&searchIn=title,description&language=en'

    response = requests.get(url)
    news_articles = response.json()

    with open(f'{home_dir}news/{stock_symbol}_news.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['source', 'author', 'title', 'description', 'url', 'urlToImage', 'publishedAt', 'content']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for article in news_articles['articles']:
            writer.writerow(article)

def update_news_content(directory):
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            count = 0
            file_path = os.path.join(directory, filename)
            
            with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)  
                
                for row in rows:
                    article_url = row['url']
                    article_content = get_article_content(article_url)
                    
                    row['content'] = article_content
                    count += 1
        
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = reader.fieldnames
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

def clean_and_sort_news_data(news_dir):
    for filename in os.listdir(news_dir):
        if filename.endswith(".csv"):
            filepath = os.path.join(news_dir, filename)
            df = pd.read_csv(filepath, parse_dates=['publishedAt'])
            
            df = df[df['content'].notnull() & (df['content'] != '')]

            df.sort_values(by='publishedAt', inplace=True)
            df.reset_index(drop=True, inplace=True)
            print(filename, df.shape[0])
            df.to_csv(filepath, index=False)

def count_articles(news_dir):
    article_counts = {}
    for filename in os.listdir(news_dir):
        if filename.endswith(".csv"):
            filepath = os.path.join(news_dir, filename)
            df = pd.read_csv(filepath)
            symbol = filename.split("_")[0]
            article_counts[symbol] = df.shape[0]
    return article_counts

def plot_article_counts(article_counts):
    symbols = list(article_counts.keys())
    counts = list(article_counts.values())
    
    plt.figure(figsize=(10, 6))
    plt.bar(symbols, counts, color='skyblue')
    plt.xlabel('Stock Symbol')
    plt.ylabel('Number of Articles')
    plt.title('Number of Articles for Each Stock Symbol')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt_file = f'{home_dir}article_counts_plot.png'
    plt.savefig(plt_file)

stocks = {
    'AAPL': 'Apple',
    'META': 'Meta',
    'AMZN': 'Amazon',
    'NFLX': 'Netflix',
    'GOOGL': 'Alphabet',
    'BTC-USD': 'Bitcoin',
    'ETH-USD': 'Ethereum',
    'BNB-USD': 'Binance Coin',
    'DOGE-USD': 'Dogecoin',
    'SOL-USD': 'Solana'
}
symbols = [ 'AAPL','META', 'AMZN', 'NFLX', 'GOOGL','BTC-USD', 'ETH-USD', 'BNB-USD', 'DOGE-USD', 'SOL-USD']
home_dir = "/home/yunusali/airflow/"

if __name__ == "__main__":
    for symbol in symbols:
        save_news_to_csv(symbol)

    update_news_content(home_dir + 'news/')
    clean_and_sort_news_data(home_dir + 'news/')
    plot_article_counts(count_articles(home_dir + 'news/'))


