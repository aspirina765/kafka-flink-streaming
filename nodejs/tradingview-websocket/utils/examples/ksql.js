const http2 = require("http2")

class KsqlDBClient {
  constructor(ksqlDBBaseUrl) {
    this.client = http2.connect(ksqlDBBaseUrl)
    this.client.on("error", error => console.error(error))
  }

  request(query) {
    const session = this.client.request({
      [http2.constants.HTTP2_HEADER_PATH]: "/query-stream",
      [http2.constants.HTTP2_HEADER_METHOD]: "POST",
      [http2.constants.HTTP2_HEADER_CONTENT_TYPE]:
        "application/vnd.ksql.v1+json",
    })

    session.setEncoding("utf8")
    session.on("data", queryResult => {
      console.log("queryResult", queryResult)
    })

    const payload = Buffer.from(JSON.stringify(query))
    session.end(payload)
  }
}

const query = {
  sql: `SELECT * FROM riderLocations
  WHERE GEO_DISTANCE(latitude, longitude, 37.4133, -122.1162) <= 5 EMIT CHANGES;`, //`SELECT * FROM test_view EMIT CHANGES;`,
}
const client = new KsqlDBClient("http://localhost:8088")
client.request(query)

const q1 = `CREATE STREAM riderLocations (profileId VARCHAR, latitude DOUBLE, longitude DOUBLE)
  WITH (kafka_topic='locations', value_format='json', partitions=1);`