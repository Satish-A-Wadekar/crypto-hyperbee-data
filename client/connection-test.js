const DHT = require('@hyperswarm/dht')

try {
    const node = new DHT({
        bootstrap: false,
        port: 30001,
        host: '127.0.0.1'
    })

    console.log('Bootstrap node running on 127.0.0.1:30001')
    node.on('listening', () => {
        console.log('Ready for connections')
    })
} catch (e) {
    console.error(e);
}
