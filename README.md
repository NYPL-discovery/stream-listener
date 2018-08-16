# Stream Listender

Simple script for reading events from a Kinesis stream and writing them to disk.

## Setup

```
npm i
```

Add credentials to `.env`:
```
cp .env-sample .env
```

## Usage

```
node stream-listen STREAMNAME [--timestamp TIMESTAMP] [--decode]
```

Options:
 * `timestamp`: Timestamp to query from
 * `decode`: If used, event payload will be Avro decoded (using schema with
     same name as stream). If absent, `Data` will be base64 encoded.
