const { connect, getCandles } = require('tradingview-ws');


(async function() {
  const connection = await connect()
  const candles = await getCandles({
    connection,
    symbols: ['FX:AUDCAD', 'FX:AUDCHF'],
    amount: 10_000,
    timeframe: 60
  })
  await connection.close()
  console.log(`Candles for AUDCAD:`, candles[0])
  console.log(`Candles for AUDCHF:`, candles[1])
}());
