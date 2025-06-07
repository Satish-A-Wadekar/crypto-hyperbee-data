'use strict'

const RPC = require('@hyperswarm/rpc')
const DHT = require('hyperdht')
const Hypercore = require('hypercore')
const Hyperbee = require('hyperbee')
const crypto = require('crypto')
const axios = require('axios')
const cron = require('node-cron')

// API configuration
const COINGECKO_API = 'https://api.coingecko.com/api/v3'
const POLL_INTERVAL = '0 */1 * * *' // Every hour at minute 0
const INITIAL_DELAY = 5000 // 5 seconds initial delay
const BACKOFF_FACTOR = 2 // Exponential backoff factor
const MAX_COINS_TO_PROCESS = 3 // Only process top 3 coins to be safe

// Rate limit state
const rateLimit = {
    remaining: 30,
    reset: 0,
    lastUpdated: 0,
    backoff: 0
}

// Priority list of coins (most important first)
const PRIORITY_COINS = [
    'bitcoin',
    'ethereum',
    'tether',
    'ripple',
    'binancecoin'
]

// Initialize database and services
const main = async () => {
    // Hyperbee database setup
    const hcore = new Hypercore('./db/crypto-server')
    const hbee = new Hyperbee(hcore, {
        keyEncoding: 'utf-8',
        valueEncoding: 'json'
    })
    await hbee.ready()

    // Generate or retrieve DHT seed
    let dhtSeed = (await hbee.get('dht-seed'))?.value
    if (!dhtSeed) {
        dhtSeed = crypto.randomBytes(32)
        await hbee.put('dht-seed', dhtSeed)
    }

    // Start DHT for service discovery
    // In the DHT configuration:
    const dht = new DHT({
        port: 40001,
        keyPair: DHT.keyPair(dhtSeed),
        bootstrap: [
            { host: '0.0.0.0', port: 30001 },
            { host: 'bootstrap1.hyperdht.org', port: 49737 },
            { host: 'bootstrap2.hyperdht.org', port: 49737 }
        ],
        relayServer: { host: '0.0.0.0', port: 49736 }  // Added relay server
    })
    await dht.ready()

    // Generate or retrieve RPC server seed
    let rpcSeed = (await hbee.get('rpc-seed'))?.value
    if (!rpcSeed) {
        rpcSeed = crypto.randomBytes(32)
        await hbee.put('rpc-seed', rpcSeed)
    }

    // Setup RPC server
    const rpc = new RPC({
        connectionKeepAlive: 30000, // 30 second keep alive
        seed: rpcSeed,
        dht
    });
    const rpcServer = rpc.createServer()
    await rpcServer.listen()
    console.log('RPC server started listening on public key:', rpcServer.publicKey.toString('hex'))

    // data collection with retries
    const fetchAndStoreData = async () => {
        console.log('\n--- Starting data fetch at', new Date().toISOString(), '---')

        try {
            // Check if we need to wait due to rate limits or backoff
            await applyRateLimitDelay()

            // Get current top coins (cached for 24 hours)
            const coins = await getTopCoins()

            // Process coins in priority order until we hit limits
            const results = await processCoinsWithPriority(coins)

            if (results.processed > 0) {
                await storeCryptoData(hbee, {
                    timestamp: Date.now(),
                    coins: results.successfulCoins,
                    exchanges: results.exchanges
                })
                console.log(`Successfully stored ${results.processed} coins`)
            }

            if (results.skipped > 0) {
                console.log(`Skipped ${results.skipped} coins due to rate limits`)
            }

        } catch (err) {
            console.error('Data pipeline failed:', err.message)
        }
    }

    // Initial fetch and schedule
    await fetchAndStoreData()
    cron.schedule(POLL_INTERVAL, fetchAndStoreData)

    // RPC methods
    rpcServer.respond('getLatestPrices', async (reqRaw) => {
        const req = JSON.parse(reqRaw.toString('utf-8'))
        const pairs = req.pairs || []
        const prices = await getLatestPrices(hbee, pairs)
        return Buffer.from(JSON.stringify(prices), 'utf-8')
    })

    rpcServer.respond('getHistoricalPrices', async (reqRaw) => {
        const req = JSON.parse(reqRaw.toString('utf-8'))
        const { pairs = [], from, to } = req
        const prices = await getHistoricalPrices(hbee, pairs, from, to)
        return Buffer.from(JSON.stringify(prices), 'utf-8')
    })

    // Expose manual trigger for testing
    rpcServer.respond('triggerDataFetch', async () => {
        await fetchAndStoreData()
        return Buffer.from(JSON.stringify({ success: true }), 'utf-8')
    })

    rpcServer.respond('ping', async () => {
        return Buffer.from(JSON.stringify({ status: 'ok' }), 'utf-8')
    })
}

// Get top coins with caching
async function getTopCoins() {
    try {
        const response = await axios.get(`${COINGECKO_API}/coins/markets`, {
            params: {
                vs_currency: 'usd',
                order: 'market_cap_desc',
                per_page: MAX_COINS_TO_PROCESS,
                page: 1
            }
        })

        // Map to our priority list order
        return PRIORITY_COINS
            .filter(coinId => response.data.some(c => c.id === coinId))
            .slice(0, MAX_COINS_TO_PROCESS)

    } catch (err) {
        console.error('Failed to get top coins, using priority list:', err.message)
        return PRIORITY_COINS.slice(0, MAX_COINS_TO_PROCESS)
    }
}

// Process coins with priority and rate limit handling
async function processCoinsWithPriority(coinIds) {
    const result = {
        processed: 0,
        skipped: 0,
        successfulCoins: [],
        exchanges: new Set()
    }

    for (const coinId of coinIds) {
        try {
            // Check if we should continue processing
            if (rateLimit.remaining <= 1 || rateLimit.backoff > 0) {
                console.log(`Stopping early due to rate limits (${coinId} not processed)`)
                result.skipped += coinIds.length - result.processed - result.skipped
                break
            }

            console.log(`Processing ${coinId}...`)
            const coinData = await processSingleCoin(coinId)

            if (coinData) {
                result.successfulCoins.push(coinData)
                coinData.exchanges.forEach(ex => result.exchanges.add(ex))
                result.processed++
            } else {
                result.skipped++
            }

        } catch (err) {
            console.error(`Error processing ${coinId}:`, err.message)
            result.skipped++

            // Apply backoff if we hit rate limits
            if (err.response?.status === 429) {
                updateRateLimits(err.response.headers)
                rateLimit.backoff = rateLimit.backoff ? rateLimit.backoff * BACKOFF_FACTOR : INITIAL_DELAY
                console.log(`Applying backoff of ${rateLimit.backoff}ms`)
            }
        }

        // Delay between coins
        await delay(rateLimit.backoff || INITIAL_DELAY)
    }

    result.exchanges = Array.from(result.exchanges).map(id => ({ id }))
    return result
}

// Process a single coin with its exchanges
async function processSingleCoin(coinId) {
    // Get tickers from top 2 exchanges only
    const response = await axios.get(`${COINGECKO_API}/coins/${coinId}/tickers`, {
        params: {
            exchange_ids: ['binance', 'coinbase-pro'].join(','),
            include_exchange_logo: false
        }
    })

    const validTickers = response.data.tickers
        .filter(t => t.target === 'USDT' && t.last)
        .map(t => ({
            exchange: t.market.name,
            exchange_id: t.market.identifier,
            price: t.last,
            volume: t.converted_volume.usd,
            timestamp: t.timestamp
        }))

    if (validTickers.length === 0) {
        console.log(`No valid tickers found for ${coinId}`)
        return null
    }

    const averagePrice = validTickers.reduce((sum, t) => sum + t.price, 0) / validTickers.length

    return {
        coin: coinId,
        symbol: coinId === 'binancecoin' ? 'BNB' : coinId.toUpperCase(),
        averagePrice,
        timestamp: Date.now(),
        exchanges: validTickers.map(t => t.exchange_id),
        tickers: validTickers
    }
}

// Rate limit delay helper
async function applyRateLimitDelay() {
    const now = Date.now()

    // Apply backoff first if exists
    if (rateLimit.backoff > 0) {
        console.log(`Applying backoff delay of ${rateLimit.backoff}ms`)
        await delay(rateLimit.backoff)
        rateLimit.backoff = 0
    }

    // Then check standard rate limits
    if (rateLimit.remaining <= 1 && now < rateLimit.reset) {
        const waitTime = Math.ceil((rateLimit.reset - now) / 1000)
        console.log(`Rate limit approaching. Waiting ${waitTime} seconds...`)
        await delay(waitTime * 1000)
    }
}

// Update rate limit tracking
function updateRateLimits(headers) {
    if (headers['x-ratelimit-remaining']) {
        rateLimit.remaining = parseInt(headers['x-ratelimit-remaining'])
    }
    if (headers['x-ratelimit-reset']) {
        rateLimit.reset = parseInt(headers['x-ratelimit-reset']) * 1000
    }
    rateLimit.lastUpdated = Date.now()
}

// Helper function to delay requests
function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
}

// Store data in Hyperbee
async function storeCryptoData(db, data) {
    // Store metadata
    await db.put('metadata', {
        lastUpdated: data.timestamp,
        coinCount: data.coins.length,
        exchangeCount: data.exchanges.length
    })

    // Store exchanges
    for (const exchange of data.exchanges) {
        await db.put(`exchange:${exchange.id}`, exchange)
    }

    // Store coin data
    for (const coin of data.coins) {
        // Store latest price
        const priceKey = `price:${coin.coin}:latest`
        await db.put(priceKey, {
            coin: coin.coin,
            symbol: coin.symbol,
            averagePrice: coin.averagePrice,
            timestamp: coin.timestamp,
            exchanges: coin.exchanges
        })

        // Store historical price
        const historyKey = `price:${coin.coin}:${coin.timestamp}`
        await db.put(historyKey, {
            coin: coin.coin,
            symbol: coin.symbol,
            averagePrice: coin.averagePrice,
            timestamp: coin.timestamp,
            exchanges: coin.exchanges,
            tickers: coin.tickers
        })
    }
}

// Get latest prices from Hyperbee
async function getLatestPrices(db, pairs) {
    const result = {}

    // If no pairs specified, get all available
    const coinsToFetch = pairs.length > 0 ? pairs : await getAvailableCoins(db)

    for (const coin of coinsToFetch) {
        const entry = await db.get(`price:${coin}:latest`)
        if (entry?.value) {
            result[coin] = entry.value
        }
    }

    return result
}

// Get historical prices from Hyperbee
async function getHistoricalPrices(db, pairs, from, to) {
    const result = {}

    // If no pairs specified, get all available
    const coinsToFetch = pairs.length > 0 ? pairs : await getAvailableCoins(db)

    for (const coin of coinsToFetch) {
        result[coin] = []

        // Create a sub to query historical data
        const sub = db.sub(`price:${coin}:`)

        for await (const entry of sub.createReadStream()) {
            const timestamp = parseInt(entry.key.split(':')[2])

            if ((!from || timestamp >= from) && (!to || timestamp <= to)) {
                result[coin].push(entry.value)
            }
        }
    }

    return result
}

// Helper to get list of available coins
async function getAvailableCoins(db) {
    const coins = new Set()

    for await (const entry of db.createReadStream({
        gt: 'price:',
        lt: 'price:~'
    })) {
        const parts = entry.key.split(':')
        if (parts.length === 3) { // historical entry
            coins.add(parts[1])
        }
    }

    return Array.from(coins)
}

main().catch(console.error)
