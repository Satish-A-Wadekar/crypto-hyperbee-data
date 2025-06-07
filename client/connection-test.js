const DHT = require('hyperdht')

try {
    const bootstrap = new DHT({
        bootstrap: false,
        port: 30001,
        host: '0.0.0.0'
    });
    console.log('Bootstrap node running on port 30001', bootstrap.bootstrapNodes);
} catch (e) {
    console.error(e);
}
