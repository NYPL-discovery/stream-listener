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
 * `stream`:    Required. Name of stream to listen to (e.g. Bib-production)
 * `timestamp`: Timestamp to query from (e.g. 2019-04-12T14:00:00.000Z).
                If used, `iterator` is ignored.
 * `iterator`:  Specify one of the following:
                - `TRIM_HORIZON`: Read events starting with the oldest event
                                  in stream.
                - `LATEST`:       Read events starting at current moment in time
                Default: `TRIM_HORIZON`
 * `decode`:    If specified, event payload will be Avro decoded and also saved
                to disk (with `.decoded` suffix) alonside default base64 event
 * `profile`:   AWS profile to use. Default 'nypl-sandbox'
 * `schema`:    Avro schema name. Defaults to name of stream (minus environment
                suffix).
