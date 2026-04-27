Cobble-flink is a lib that integrate [Cobble](https://github.com/cobble-project/cobble) with [Apache Flink®](https://flink.apache.org/), which allows you to use Cobble in Flink applications.
It provides a state backend, sink and source for Flink, which can be used to store and process data in Cobble.

## Configuration

`cobble-state` maps a curated subset of Cobble runtime settings into Flink configuration under
`state.backend.cobble.*`, including:

- `state.backend.cobble.memtable.type`
- `state.backend.cobble.compaction.policy`
- `state.backend.cobble.sst.bloom-filter.enabled`
- `state.backend.cobble.sst.bloom-filter.bits-per-key`
- `state.backend.cobble.sst.partitioned-index.enabled`
- `state.backend.cobble.value-separation.threshold`
- `state.backend.cobble.direct-io.buffer-size`
- `state.backend.cobble.direct-io.pool-max-size`
- `state.backend.cobble.log.level`
- `state.backend.cobble.log.max-file-size`
- `state.backend.cobble.log.keep-files`
- `state.backend.cobble.snapshot.retention`

Internal bootstrap details such as Cobble's default column count are intentionally not exposed.

When Flink checkpoints use remote filesystems, Cobble also mirrors the matching filesystem
credentials into its checkpoint volume config. Today this includes:

- S3 / S3A style keys such as `s3.access-key`, `s3.secret-key`, `s3.endpoint`,
  `s3.path.style.access`, `fs.s3a.access.key`, and `fs.s3a.secret.key`
- OSS keys such as `fs.oss.accessKeyId`, `fs.oss.accessKeySecret`, and `fs.oss.endpoint`

## License

This project is licensed under the Apache-2.0 License. See the [LICENSE](LICENSE) file for details.

## Notice

The [Apache Flink®](https://flink.apache.org/) is registered trademarks of The Apache Software Foundation in the United States and other countries. 
