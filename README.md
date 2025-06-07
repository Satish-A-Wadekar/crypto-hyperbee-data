# crypto-hyperbee-data

a simple crypto currency data gathering solution using Hyperswarm RPC and Hypercores.

Key Features Implemented
Data Collection:
 -  Fetches top 5 cryptocurrencies by market cap from CoinGecko
 -  Gets prices from top 3 exchanges for each coin
 -  Calculates average price across exchanges

Data Storage:
 -  Uses Hyperbee for efficient storage
 -  Stores minimal necessary data (coin, price, timestamp, exchanges)
 -  Maintains both latest and historical prices

Scheduling:
 -  Runs every 30 seconds using node-cron
 -  Can be triggered manually via RPC

Data Exposure:
-  Implements getLatestPrices and getHistoricalPrices RPC methods
-  Supports filtering by coin pairs and date ranges

Data Quality:
-  Filters out invalid tickers (missing prices or wrong target currency)
-  Calculates average only from valid prices
