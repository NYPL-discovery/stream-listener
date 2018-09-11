/**
 *
 * Listen to a named kinesis stream, optionally from a timestamp.
 * Decode payloads of received events, and write the resultant json to disk.
 *
 * Usage:
 *   node stream-listen --stream STREAMNAME [--timestamp TIMESTAMP] [--decode] [--profile PROFILE] [--schema SCHEMANAME]
 */
const path = require('path')
const fs = require('fs')
const AWS = require('aws-sdk')

const NyplClient = require('@nypl/nypl-data-api-client')
const dataApiClient = new NyplClient({ base_url: 'http://platform.nypl.org/api/v0.1/' })

const argv = require('minimist')(process.argv.slice(2))

AWS.config.credentials = new AWS.SharedIniFileCredentials({ profile: argv.profile || 'nypl-sandbox' })

const streamName = argv.stream
const schemaName = argv.schema || streamName

// Set aws region:
let awsSecurity = { region: 'us-east-1' }
AWS.config.update(awsSecurity)

const client = new AWS.Kinesis({
  region: 'us-east-1',
  params: { StreamName: streamName }
})

var options = {
  shardId: argv.shardId || 'shardId-000000000001',
  // iterator: 'LATEST', // default to TRIM_HORIZON
  // startAfter: '12345678901234567890', // start reading after this sequence number
  // startAt: '12345678901234567890', // start reading from this sequence number
  // timestamp: argv.timestamp || (new Date()).toISOString(),
  limit: 100 // number of records per `data` event
}

if (argv.timestamp) options.timestamp = argv.timestamp

// see below for options
const kinesisReadable = require('kinesis-readable')(client, options)

const destinationDirectory = './data'
if (!fs.existsSync(destinationDirectory)) fs.mkdirSync(destinationDirectory)

dataApiClient.get(`current-schemas/${schemaName}`, { authenticate: false }).then((resp) => {
  let schema = resp.data
  // Now we can build an avro encoder by parsing the escaped "schema" prop:
  var avroType = require('avsc').parse(JSON.parse(schema.schema))

  kinesisReadable
    // 'data' events will trigger for a set of records in the stream
    .on('data', function (events) {
      events.map((ev, ind) => {
        let recordSummary = ev.Data.toString().substring(0, 20) + (ev.Data.length > 20 ? '...' : '')

        if (argv.decode) {
          // Decode avro encoded data
          const buf = new Buffer(ev.Data, 'base64')
          ev.Data = avroType.fromBuffer(buf)

          // Build truncated record for display containing only the following
          // interesting identifiers:
          const truncatedRecord = ['id', 'nyplSource', 'uri', 'type'].reduce((h, prop) => {
            if (ev.Data[prop]) h[prop] = ev.Data[prop]
            return h
          }, {})
          recordSummary = JSON.stringify(truncatedRecord)
          // If our summary hides keys, print an ellipses to indicate that:
          if (Object.keys(ev.Data) > Object.keys(truncatedRecord)) {
            recordSummary = recordSummary.replace(/}$/, ' ...}')
          }
        } else {
          ev.Data = ev.Data.toString('base64')
        }

        console.log(`${ev.ApproximateArrivalTimestamp}: ${schemaName} ${ind}: ${recordSummary}`)

        // Establish folder to write to:
        const baseDir = path.join(destinationDirectory, streamName)
        // Ensure folder exists:
        if (!fs.existsSync(baseDir)) fs.mkdirSync(baseDir)

        // Establish event filename:
        const filename = [ev.PartitionKey, ev.SequenceNumber, ind].join('-') +
          (argv.decode ? '.decoded' : '') +
          '.json'

        const writePath = path.join(baseDir, filename)
        console.log(`  > ${writePath}`)

        if (!fs.existsSync(writePath)) fs.writeFileSync(writePath, JSON.stringify(ev, null, 2))
      })
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
  kinesisReadable.close()
}, 60 * 60 * 1000)
