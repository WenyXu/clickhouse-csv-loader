## Generate logs
```bash
./flog -n 1000000 -S 11 -f csv -o test.log -t log 
```

## Create table
```bash
CREATE TABLE IF NOT EXISTS `logs` (
  `host` String,
  `user-identifier` String,
  `datetime` DateTime,
  `method` String,
  `request` String,
  `protocol` String,
  `status` UInt64,
  `bytes` UInt64,
  `referer` String,
  `user-agent` String,
)
ENGINE = MergeTree
ORDER BY (toStartOfHour(datetime), status, request, host)
```

## Import logs to ClickHouse
```bash
./clickhouse-csv-loader --csv ./test.log --table logs
```

