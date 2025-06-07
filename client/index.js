'use strict'

const RPC = require('@hyperswarm/rpc')
const DHT = require('hyperdht')
const Hypercore = require('hypercore')
const Hyperbee = require('hyperbee')
const crypto = require('crypto')

const main = async () => {
    let rpc, dht;
    try {
        // Hyperbee database setup
        const hcore = new Hypercore('./db/crypto-client')
        const hbee = new Hyperbee(hcore, {
            keyEncoding: 'utf-8',
            valueEncoding: 'binary'
        })
        await hbee.ready()

        // Generate or retrieve DHT seed
        let dhtSeed = (await hbee.get('dht-seed'))?.value
        if (!dhtSeed) {
            dhtSeed = crypto.randomBytes(32)
            await hbee.put('dht-seed', dhtSeed)
        }

        // Start DHT for service discovery
        dht = new DHT({
            port: 50001,
            keyPair: DHT.keyPair(dhtSeed),
            bootstrap: [
                { host: '127.0.0.1', port: 30001 }
            ]
        })
        await dht.ready()

        // Replace with your server's public key (from server console output)
        const serverPubKey = Buffer.from('3b6a27bcceb6a42d62a3a8d02a6f0d73653215771de243a63ac048a18b59da29', 'hex')

        // RPC client with connection timeout
        rpc = new RPC({
            dht,
            connectionTimeout: 10000 // 10 second timeout
        })

        // Ensure we have a connection before making requests
        await ensureConnection(rpc, dht, serverPubKey)

        // Example: Get latest prices
        try {
            console.log('\nFetching latest prices...')
            const latestReq = Buffer.from(JSON.stringify({
                pairs: ['bitcoin', 'ethereum'] // Can be empty to get all
            }), 'utf-8')

            const latestRes = await rpc.request(serverPubKey, 'getLatestPrices', latestReq, {
                timeout: 8000 // 8 second timeout
            })
            const latestPrices = JSON.parse(latestRes.toString('utf-8'))
            console.log('Latest prices:', latestPrices)
        } catch (err) {
            console.error('Error getting latest prices:', err.message)
        }

        // Example: Get historical prices
        try {
            console.log('\nFetching historical prices...')
            const now = Date.now()
            const oneHourAgo = now - (60 * 60 * 1000)

            const historicalReq = Buffer.from(JSON.stringify({
                pairs: ['bitcoin'], // Can be empty to get all
                from: oneHourAgo,
                to: now
            }), 'utf-8')

            const historicalRes = await rpc.request(serverPubKey, 'getHistoricalPrices', historicalReq, {
                timeout: 8000 // 8 second timeout
            })
            const historicalPrices = JSON.parse(historicalRes.toString('utf-8'))
            console.log('Historical prices:', historicalPrices)
        } catch (err) {
            console.error('Error getting historical prices:', err.message)
        }

    } catch (err) {
        console.error('Client error:', err)
    } finally {
        // Clean up
        if (rpc) await rpc.destroy()
        if (dht) await dht.destroy()
        process.exit(0)
    }
}

// Updated connection helper without dht.lookup
async function ensureConnection(rpc, dht, serverPubKey) {
    let attempts = 0
    const maxAttempts = 5
    const retryDelay = 2000

    while (attempts < maxAttempts) {
        try {
            // Try a simple ping to verify connection
            const pingRes = await rpc.request(serverPubKey, 'ping', Buffer.from('{}'), {
                timeout: 5000
            })
            const response = JSON.parse(pingRes.toString())
            if (response.status !== 'ok') {
                throw new Error('Invalid ping response')
            }
            return true
        } catch (err) {
            attempts++
            if (attempts >= maxAttempts) {
                throw new Error(`Connection failed after ${maxAttempts} attempts: ${err.message}`)
            }
            console.log(`Attempt ${attempts} failed (${err.message}), retrying in ${retryDelay/1000}s...`)
            await new Promise(resolve => setTimeout(resolve, retryDelay))
        }
    }
}

main()