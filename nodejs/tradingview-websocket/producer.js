// import the `Kafka` instance from the kafkajs library
const { Kafka } = require("kafkajs")

// the client ID lets kafka know who's producing the messages
const clientId = "my-app"
// we can define the list of brokers in the cluster
const brokers = ["localhost:9092"]
// this is the topic to which we want to write messages
const topic = "message-log"

// initialize a new kafka client and initialize a producer from it
const kafka = new Kafka({ clientId, brokers })
const { connect, getCandles } = require('tradingview-ws');
const producer = kafka.producer()

// we define an async function that writes a new message each second
const produce = async () => {
    const connection = await connect()
    const candles = await getCandles({
      connection,
      symbols: ['FX:AUDCAD'],
      amount: 1, // 10_000,
      timeframe: 60
    })

	await producer.connect()
	let i = 0

	// after the produce has connected, we start an interval timer
	setInterval(async () => {
		try {
			// send a message to the configured topic with
			// the key and value formed from the current value of `i`
			await producer.send({
				topic,
				messages: [
					{
						key: String(i),
						value: "this is message " + JSON.stringify(candles[0]),//i,
					},
				],
			})
            await connection.close()
			// if the message is written successfully, log it and increment `i`
			console.log("writes: ", i)
			i++
		} catch (err) {
			console.error("could not write message " + err)
		}
	}, 1000)
}

module.exports = produce