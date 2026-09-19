# Debezium connector registration

`register-txn-connector.json` defines the Debezium Postgres connector that watches
`public.txn` and publishes one change event per row change to `sentinel.public.txn`. You
register it by POSTing it to a running Kafka Connect instance. Connect owns the connector,
not this repo.

Two things must be true before any of this works. Postgres must run with
`wal_level=logical`, and a Kafka Connect instance carrying the Debezium Postgres connector
must exist. Neither is true of the stock `personal-infra` setup. See **How to set up
Debezium** in the [engine README](../README.md).

## Register the connector

If Connect runs in Docker, first change `database.hostname` from `localhost` to the Postgres
container's name, which is `postgres` on the `personal-infra` compose network. Inside the
Connect container, `localhost` is the Connect container itself.

```bash
curl -i -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connector/register-txn-connector.json
```

`201 Created` means Connect accepted the config, not that the connector is healthy. Check
the status next.

## Check the status

```bash
curl -s http://localhost:8083/connectors/sentinel-txn-connector/status | python3 -m json.tool
```

`connector.state` and every `tasks[].state` must read `RUNNING`. A `FAILED` task carries the
stack trace in its `trace` field. A `wal_level` that is not `logical` surfaces there.

To list everything Connect currently runs and confirm the topic appeared, run:

```bash
curl -s http://localhost:8083/connectors
docker exec personal-infra-kafka-1 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

To read a few change events straight off the topic, run:

```bash
docker exec personal-infra-kafka-1 \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic sentinel.public.txn --from-beginning --max-messages 5
```

## Update the connector

Connect has no edit operation. To replace the config in place, PUT the inner `config`
object on its own, without the surrounding `name` and `config` wrapper:

```bash
curl -i -X PUT http://localhost:8083/connectors/sentinel-txn-connector/config \
  -H "Content-Type: application/json" \
  -d "$(python3 -c 'import json,sys; print(json.dumps(json.load(open("connector/register-txn-connector.json"))["config"]))')"
```

## Delete the connector

```bash
curl -i -X DELETE http://localhost:8083/connectors/sentinel-txn-connector
```

**Deleting the connector does not drop its replication slot.** Postgres keeps retaining WAL
for `sentinel_engine` until you drop the slot, and that retained WAL will eventually fill
the disk. When you are done with the connector for good, drop the slot too:

```bash
docker exec personal-infra-postgres-1 psql -U catalog -d sentinel \
  -c "select pg_drop_replication_slot('sentinel_engine')"
```

Leave the slot in place only if you intend to re-register the connector and want it to
resume from where it stopped rather than snapshot the table again.
