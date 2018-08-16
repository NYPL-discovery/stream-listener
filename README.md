# Stream Listener

Simple script for reading events from a Kinesis stream and writing them to disk.

## Setup

```
npm i
```

## Usage

```
node stream-listen --stream STREAMNAME [--timestamp TIMESTAMP] [--decode] [--profile PROFILE] [--schema SCHEMA]
```

Options:
 * `stream`: Required. Name of stream to listen to (e.g. Bib-production)
 * `timestamp`: Timestamp to query from
 * `decode`: If used, event payload will be Avro decoded (using schema with
     same name as stream). If absent, `Data` will be base64 encoded.
 * `profile`: AWS profile to use. Default 'nypl-sandbox'
 * `schema`: Avro schema name. Defaults to name of stream.
