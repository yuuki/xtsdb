# Xtsdb

Xtsdb is a scalable time series database on two tiers of Redis and Cassandra.

## Architecture

<img alt="xtsdb-architecture" src="https://github.com/yuuki/xtsdb/raw/master/docs/images/architecture.png" width="800">

## Requirements

- Memory-based KVS
  - Redis 5.0+
- Disk-based KVS
  - Apache Cassandra 3.0+
  - Apache HBase (To be added)
  - Amazon DynamoDB (To be added)
  - Amazon S3 (To be added)
  - Google BigTable (To be added)
  - Google Cloud Storage (To be added)

## Experiments

[xtsdb-experiments](https://github.com/yuuki/xtsdb-experiments)

## Papers

1. <u>坪内佑樹</u>, 脇坂朝人, 濱田健, 松木雅幸, 小林隆浩, 阿部博, 松本亮介, **[HeteroTSDB: 異種分散KVS間の自動階層化による高性能な時系列データベース](http://id.nii.ac.jp/1001/00210244/)**, 情報処理学会論文誌, Vol.62, No.3, pp.818-828, 2021年3月. [[paper](https://yuuk.io/papers/2021/heterotsdb_ipsj_journal.pdf)] [[code](https://github.com/yuuki/xtsdb)]

## License

```
Copyright 2020 Yuuki Tsubouchi

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## Author

[yuuki](https://github.com/yuuki)
