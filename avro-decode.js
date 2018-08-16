/**
 * Usage:
 *   node avro-decode --schema SCHEMANAME --value VALUE
 *
 * Example:
 *   node avro-decode.js --schema SierraBibRetrievalRequest --value EDEyMjg5NzE1
 */
const NyplClient = require('@nypl/nypl-data-api-client')
const dataApiClient = new NyplClient({ base_url: 'http://platform.nypl.org/api/v0.1/' })

const argv = require('minimist')(process.argv.slice(2))

dataApiClient.get(`current-schemas/${argv.schema}`, { authenticate: false }).then((resp) => {
  let schema = resp.data
  var avroType = require('avsc').parse(JSON.parse(schema.schema))

  // Decode avro encoded data
  const buf = Buffer.from(argv.value, 'base64')
  const decoded = avroType.fromBuffer(buf)

  console.log('Decoded: ', decoded)
})
