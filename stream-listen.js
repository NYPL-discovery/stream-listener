/**
 *
 * Listen to a named kinesis stream, optionally from a timestamp.
 * Decode payloads of received events, and write the resultant json to disk.
 *
 * Usage:
 *   node stream-listen --stream STREAMNAME [--timestamp TIMESTAMP] [--decode] [--profile PROFILE] [--schema SCHEMANAME] [--toEvent EVENTFILE]
 */
const path = require('path')
const fs = require('fs')
const AWS = require('aws-sdk')

const NyplClient = require('@nypl/nypl-data-api-client')
const dataApiClient = new NyplClient({ base_url: 'http://platform.nypl.org/api/v0.1/' })

const argv = require('minimist')(process.argv.slice(2))

AWS.config.credentials = new AWS.SharedIniFileCredentials({ profile: argv.profile || 'nypl-sandbox' })

const streamName = argv.stream
const schemaName = argv.schema || streamName.replace(/-\w+$/, '')

// Set aws region:
let awsSecurity = { region: 'us-east-1' }
AWS.config.update(awsSecurity)

const client = new AWS.Kinesis({
  region: 'us-east-1',
  params: { StreamName: streamName }
})

// Given a streamName (e.g. "Item-production"), returns an array of shardIds)
function shardIdsForStream (streamName) {
  return new Promise((resolve, reject) => {
    client.describeStream({ StreamName: streamName }, function(err, data) {
      if (err) return reject(err)

      resolve(data.StreamDescription.Shards.map((shard) => shard.ShardId))
    })
  })
}

const collectedDecoded = []

function eventsAsEventJson (records, streamName) {
  return {
    "Records": records.map((record, i) => {
      return {
        "kinesis": {
          "PartitionKey": record.PartitionKey,
          "kinesisSchemaVersion": "1.0",
          "SequenceNumber": record.SequenceNumber,
          "ApproximateArrivalTimestamp": record.ApproximateArrivalTimestamp,
          "data": record.Data
        },
        "eventSource": "aws:kinesis",
        "eventID": "shardId-000000000000:49545115243490985018280067714973144582180062593244200961",
        "invokeIdentityArn": "arn:aws:iam::EXAMPLE",
        "eventVersion": "1.0",
        "eventName": "aws:kinesis:record",
        "eventSourceARN": `arn:aws:kinesis:us-east-1::stream/${streamName}`,
        "awsRegion": "us-east-1"
      }
    })
  }
}

let readCount = 0
function readStream (options) {
  const kinesisReadable = require('kinesis-readable')(client, options)
  dataApiClient.get(`current-schemas/${schemaName}`, { authenticate: false }).then((resp) => {
    let schema = resp.data

    // Now we can build an avro encoder by parsing the escaped "schema" prop:
    var avroType = require('avsc').parse(JSON.parse(schema.schema))

    kinesisReadable
      // 'data' events will trigger for a set of records in the stream
      .on('data', function (events) {
        console.log('Got ' + events.length + ' events')
        readCount += 1

        events.map((ev, ind) => {
          let recordSummary = ev.Data.toString().substring(0, 20) + (ev.Data.length > 20 ? '...' : '')
          let decoded = null

          if (argv.decode) {
            // Decode avro encoded data
            const buf = new Buffer(ev.Data, 'base64')
            decoded = avroType.fromBuffer(buf)

            // Build truncated record for display containing only the following
            // interesting identifiers:
            let props = ['id', 'nyplSource', 'uri', 'type']
            if (argv.pluck) props = props.concat(argv.pluck.split(','))
            const truncatedRecord = props.reduce((h, prop) => {
              if (decoded[prop]) h[prop] = decoded[prop]
              return h
            }, {})
            recordSummary = JSON.stringify(truncatedRecord)
            // If our summary hides keys, print an ellipses to indicate that:
            if (Object.keys(decoded) > Object.keys(truncatedRecord)) {
              recordSummary = recordSummary.replace(/}$/, ' ...}')
            }

          } 

          if (argv.timestampstop && new Date(argv.timestampstop) < new Date(ev.ApproximateArrivalTimestamp)) {
            console.log('Reached timestamp stop: ', argv.timestampstop)
            if (argv.tocsv) {
              writeToCsv(collectedDecoded, argv.tocsv)
            }
            process.exit()
          }

          if (argv.tocsv) {
            collectedDecoded.push(decoded)
          }

          ev.Data = ev.Data.toString('base64')

          const shardLabel = options.shardId ? `${options.shardId}:` : ''
          console.log(`${ev.ApproximateArrivalTimestamp}: ${schemaName} ${shardLabel}${ind}: ${recordSummary}`)

          // Establish folder to write to:
          const baseDir = path.join(destinationDirectory, streamName)
          // Ensure folder exists:
          if (!fs.existsSync(baseDir)) fs.mkdirSync(baseDir)

          // Establish event filename:
          const filename = [ev.PartitionKey, ev.SequenceNumber, ind].join('-')

          let writePath = path.join(baseDir, filename + '.json')
          console.log(`  > ${writePath}`)
          if (!fs.existsSync(writePath)) fs.writeFileSync(writePath, JSON.stringify(ev, null, 2))

          if (argv.decode) {
            writePath = path.join(baseDir, filename + '.decoded.json')
            console.log(`  > ${writePath}`)
            if (!fs.existsSync(writePath)) fs.writeFileSync(writePath, JSON.stringify(decoded, null, 2))
          }

          collectedEvents.push(ev)

          if (argv.toEvent) {
            fs.writeFileSync(argv.toEvent, JSON.stringify(eventsAsEventJson(collectedEvents, streamName), null, 2))
          }
        })

        // Write to csv periodically just in case we fail..
        if (argv.tocsv && readCount % 50 === 1) {
          writeToCsv(collectedDecoded, argv.tocsv)
        }
      })
      // each time a records are passed downstream, the 'checkpoint' event will provide
      // the last sequence number that has been read
      .on('checkpoint', function (sequenceNumber) {
        console.log('Sequence number: ', sequenceNumber)
      })
      .on('error', function (err) {
        console.error('Error!', err)
      })
      .on('end', function () {
        console.log('All done!')
      })
  })

  // Calling .close() will finish all pending GetRecord requests before emitting
  // the 'end' event.
  // Because the kinesis stream persists, the readable stream will not
  // 'end' until you explicitly close it
  setTimeout(function () {
    console.log('Closing because it has been a while')
    // kinesisReadable.close()
  }, 60 * 60 * 1000)
}

const collectedEvents = []

const options = {
  // shardId: argv.shardId || 'shardId-000000000000',
  // iterator: 'LATEST', // default to TRIM_HORIZON
  // startAfter: '12345678901234567890', // start reading after this sequence number
  // startAt: '12345678901234567890', // start reading from this sequence number
  limit: 100 // number of records per `data` event
}

if (argv.timestamp) {
  let iso8601Timestamp = null
  try {
    iso8601Timestamp = new Date(argv.timestamp).toISOString()
  } catch (e) {
    console.log(`Error parsing timestamp: ${argv.timestamp}: ${e}`)
    process.exit()
  }
  console.log(`Interpretting timestamp "${argv.timestamp}" as ${iso8601Timestamp}`)
  argv.timestamp = iso8601Timestamp
}

function writeToCsv (events, file) {
  console.log(`Writing ${events.length} events to ${argv.tocsv}`)
  const cols = Object.keys(events[0])

  const rows = events
    .map((ev) => cols.map((c) => ev[c]))
    .map((row) => row.join(','))

  const content = rows.join("\n")
  fs.writeFileSync(argv.tocsv, content)
}

if (argv.timestamp) options.timestamp = argv.timestamp
else options.iterator = argv.iterator || 'TRIM_HORIZON'

console.log('Using options: ', options)

const destinationDirectory = './data'
if (!fs.existsSync(destinationDirectory)) fs.mkdirSync(destinationDirectory)

shardIdsForStream(argv.stream).then((shardIds) => {
  console.log(`Reading from ${shardIds.length} shard(s): ${shardIds.join(", ")}`)

  shardIds.forEach((shardId) => {
    const shardIdOptions = Object.assign({}, options, { shardId })
    readStream(shardIdOptions)
  })
})

