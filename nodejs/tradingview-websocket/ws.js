const { connect, getCandles } = require('tradingview-ws');
const { writeFile, write } = require('fs');
// const { writeFiles } = require('./write');


// const path = './config.json';
// const config = { ip: '192.0.2.10', port: 3000 };


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

  const path = './config.json';
  const config = candles[0]; // { ip: '192.0.2.10', port: 3000 };
  
  const writeFiles = async () => writeFile(path, JSON.stringify(config, null, 2), (error) => {
    if (error) {
      console.log('An error has occurred ', error);
      return;
    }
    console.log('Data written successfully to disk');
  });
  writeFiles();
  console.log(`Candles for AUDCHF:`, candles[1])
}());
