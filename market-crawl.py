import time
import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException
from datetime import datetime
import os
import threading
from queue import Queue

from library import lib_initialize, get_binance_obj


class BinanceDataCrawler:
    def __init__(self, api_key, api_secret, num_threads=4):
        """Initialize the Binance client with API credentials"""
        self.client = Client(api_key, api_secret)
        self.base_path = "/home/simon/data/my/crypto/klines"
        self.num_threads = num_threads
        self.symbol_queue = Queue()

        # Create directory if it doesn't exist
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path)

    def get_usdt_pairs(self):
        """Get all trading pairs with USDT as quote asset"""
        try:
            exchange_info = self.client.get_exchange_info()
            symbols = exchange_info['symbols']

            usdt_pairs = [
                symbol['symbol']
                for symbol in symbols
                if symbol['quoteAsset'] == 'USDT'
                   and symbol['status'] == 'TRADING'
            ]

            print(f"Found {len(usdt_pairs)} USDT trading pairs")
            return usdt_pairs

        except Exception as e:
            print(f"Error getting USDT pairs: {str(e)}")
            return []

    def process_symbol(self, symbol):
        """Process a single symbol (called by worker threads)"""
        intervals = [
            '15m', '30m', '1h', '2h', '4h',
            '6h', '8h', '12h', '1d', '2d',
            '3d', '1w'
        ]

        print(f"\n[Thread {threading.current_thread().name}] Processing {symbol}")
        symbol_path = os.path.join(self.base_path, symbol)

        # Create symbol directory if it doesn't exist
        if not os.path.exists(symbol_path):
            try:
                os.makedirs(symbol_path)
            except PermissionError as e:
                print(f"Permission denied creating directory for {symbol}: {str(e)}")
                return
            except Exception as e:
                print(f"Error creating directory for {symbol}: {str(e)}")
                return

        # Verify ticker exists
        try:
            self.client.get_symbol_info(symbol)
        except BinanceAPIException as e:
            print(f"Invalid ticker {symbol}: {str(e)}")
            return
        except Exception as e:
            print(f"Error verifying {symbol}: {str(e)}")
            return

        for interval in intervals:
            try:
                print(f"[Thread {threading.current_thread().name}] Fetching {interval} klines for {symbol}")

                klines = self.client.get_historical_klines(
                    symbol=symbol,
                    interval=interval,
                    start_str="1 Jan, 2017",
                    end_str=None,
                    limit=1000
                )

                if not klines:
                    print(f"No data available for {symbol} - {interval}")
                    filename = f"{symbol}_{interval}.csv"
                    filepath = os.path.join(symbol_path, filename)
                    pd.DataFrame().to_csv(filepath)
                    continue

                # Convert to DataFrame
                df = pd.DataFrame(klines, columns=[
                    'timestamp', 'open', 'high', 'low', 'close',
                    'volume', 'close_time', 'quote_volume',
                    'trades', 'taker_base_volume', 'taker_quote_volume',
                    'ignore'
                ])

                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')

                numeric_cols = ['open', 'high', 'low', 'close', 'volume',
                                'quote_volume', 'taker_base_volume',
                                'taker_quote_volume']
                df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
                df = df.drop('ignore', axis=1)

                # Save to CSV
                filename = f"{symbol}_{interval}.csv"
                filepath = os.path.join(symbol_path, filename)
                df.to_csv(filepath, index=False)
                print(f"[Thread {threading.current_thread().name}] Saved {len(df)} rows to {filepath}")

                time.sleep(0.5)  # Rate limiting per request

            except BinanceAPIException as e:
                print(f"API Error for {symbol} - {interval}: {str(e)}")
                filename = f"{symbol}_{interval}.csv"
                filepath = os.path.join(symbol_path, filename)
                pd.DataFrame().to_csv(filepath)
                time.sleep(2)
            except PermissionError as e:
                print(f"Permission denied writing {symbol} - {interval}: {str(e)}")
                time.sleep(2)
            except Exception as e:
                print(f"Unexpected error for {symbol} - {interval}: {str(e)}")
                time.sleep(2)

    def worker(self):
        """Worker thread function"""
        while True:
            try:
                symbol = self.symbol_queue.get_nowait()
            except:
                break

            self.process_symbol(symbol)
            self.symbol_queue.task_done()

    def crawl_historical_klines(self, symbol_list=None):
        """Crawl historical klines using multiple threads for all USDT pairs"""
        # Use all USDT pairs if no specific list provided
        symbol_list = self.get_usdt_pairs()

        if not symbol_list:
            print("No symbols to process")
            return

        # Fill the queue with symbols
        for symbol in symbol_list:
            self.symbol_queue.put(symbol)

        # Create and start threads
        threads = []
        for i in range(min(self.num_threads, len(symbol_list))):
            t = threading.Thread(target=self.worker, name=f"Worker-{i}")
            t.start()
            threads.append(t)

        # Wait for all threads to complete
        for t in threads:
            t.join()

        print("\nAll USDT markets processed")


if __name__ == "__main__":
    # Replace with your Binance API credentials
    # Replace with your Binance API credentials
    lib_initialize()
    binance_obj = get_binance_obj()
    # Replace with your Binance API credentials
    API_KEY = binance_obj.key_api
    API_SECRET = binance_obj.key_secret

    # Create crawler instance with 4 threads
    crawler = BinanceDataCrawler(API_KEY, API_SECRET, num_threads=4)

    # Crawl all USDT markets
    crawler.crawl_historical_klines()

