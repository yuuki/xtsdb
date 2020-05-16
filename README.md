# Xtsdb

Xtsdb is a scalable time series database on two tiers of Redis and Cassandra.

## Architecture

<img alt="xtsdb-architecture" src="https://github.com/yuuki/xtsdb/raw/master/docs/images/architecture.png" width="800">

## Requirements

- Memory-based KVS
  - Redis 5.0+
- Disk-based KVS
  - Apache Cassandra 4.0+
  - Apache HBase (To be added)
  - Amazon DynamoDB (To be added)
  - Amazon S3 (To be added)
  - Google BigTable (To be added)
  - Google Cloud Storage (To be added)

## License

[MIT](LICENSE)

## Author

[yuuki](https://github.com/yuuki)
