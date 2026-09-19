# Sentinel AML Engine

## Demo

<video src="https://github.com/Tush1810/sentinel-aml-engine/raw/main/docs/sentinel-demo.mp4" controls width="100%"></video>

If the player does not load, [open the recording](docs/sentinel-demo.mp4) directly.

## 1. What the engine does

The engine is a Kafka consumer that runs continuously and serves no HTTP
(`spring.main.web-application-type: none`). It consumes row-level change events for the
`txn` table, re-reads each transaction from Postgres, evaluates the rule book against that
transaction, and writes alerts.

### Why detection runs in its own process

Detection used to run inside the ingestion service, on the same thread as the HTTP request
that accepted the transaction. Splitting it into a separate process gives you three things:

- **Ingestion latency no longer depends on rule cost.** A windowed rule reads a day of
  history for every transaction. That work no longer sits inside the caller's request.
- **You can tune the rule book and restart it without touching ingestion.** The engine binds
  `rules.yml` once at startup, so a threshold change means restarting the engine alone. The
  ingestion API stays up.
- **No transaction can be stored and never evaluated.** The trigger is the write-ahead log,
  not an application-level publish. Postgres cannot commit a row without writing that row to
  the WAL, so no window exists in which a transaction sits in the database with no
  evaluation pending for it. An application that persists a row and then publishes its own
  event has exactly that window whenever it crashes in between.

The price is that detection is now asynchronous. `POST /api/v1/transactions` on the
ingestion service returns before any alert exists.

## 2. How a transaction reaches the engine

Sentinel is three repositories.

| Repo | Role |
|---|---|
| `sentinel-aml-service` | Ingestion (CSV, REST, and Kafka), alert queue API, case management, dashboard backend. **Owns the Postgres schema and its Flyway migrations.** Writes rows to `txn`. Runs no detection. |
| `sentinel-aml-engine` | **This repo.** A Kafka consumer with no HTTP. Consumes Debezium change events for `txn`, evaluates the rule book, and writes the `alert` and `alert_evidence` tables. |
| `sentinel-aml-dashboard` | React UI over the ingestion service's REST API. |

A transaction travels this path:

```
   CSV upload, REST POST, Kafka          sentinel-aml-service
                 |                       (ingest, normalize currency, persist)
                 v
        +------------------+
        |    Postgres      |  INSERT into txn  ->  written to the WAL
        |   (txn table)    |
        +------------------+
                 |
                 | logical decoding (pgoutput), replication slot `sentinel_engine`
                 v
        +------------------+
        |  Kafka Connect   |  Debezium Postgres connector
        |  (Debezium)      |  reads the WAL, one change event per row change
        +------------------+
                 |
                 v
        Kafka topic `sentinel.public.txn`
                 |
                 v
        +----------------------------+
        | sentinel-aml-engine        |
        | TransactionChangeListener  |  op=c (insert) or op=r (snapshot read)
        |        |                   |
        |        v                   |
        | TransactionRepository      |  re-reads the row (needs account, customer, window)
        |        |                   |
        |        v                   |
        | DetectionEngine            |  every enabled rule from rules.yml
        |        |                   |
        |        v                   |
        | AlertService               |  risk score, severity, dedup by natural key
        +----------------------------+
                 |
                 v
        the alert and alert_evidence tables, read back by the ingestion service's alert queue
```

The engine treats the change event purely as a **notification that a row exists**.
`TransactionChangeListener` pulls `after.id` out of the envelope, throws the rest away, and
re-reads the transaction through JPA. Detection needs the account, the customer, and the
surrounding time window, and the change payload carries none of those. Because the listener
re-reads the row, the connector's `value.converter.schemas.enable` setting does not matter
to the engine. The listener accepts the envelope with or without its schema wrapper.

### Where each class lives

The packages sit under `src/main/java/com/tushar/sentinel/`.

| Package | Role |
|---|---|
| `consumer/` | `TransactionChangeListener`, the only entry point into the process |
| `service/detection/` | `DetectionEngine` (rule evaluation, four shapes) and `AlertService` (scoring, severity, dedup) |
| `service/` | `SentinelProperties`, the typed binding of `rules.yml` |
| `repository/` | JPA entities and Spring Data repositories, one sub-package per aggregate (`customer`, `account`, `txn`, `alert`) |
| `SentinelEngineApplication` | Boot entry point, carrying `@ConfigurationPropertiesScan` |

The entities mirror the ingestion service's schema, and `spring.jpa.hibernate.ddl-auto` is
`validate`. **The engine never creates or migrates a table.** When validation fails at
startup, nobody has run the ingestion service's Flyway migrations against this database.

## 3. The rule book is YAML, not Java

Detection rules live entirely in `src/main/resources/rules.yml`. Spring binds them at
startup into typed records (`SentinelProperties.Rule`) through `@ConfigurationProperties`.
Tuning a threshold, a window, or a weight is a YAML edit, and so is adding a whole new rule.
No Java, no rebuild. `DetectionEngine` reduces every rule to one of four reusable shapes and
switches on `type`.

| Type | What it measures | Rules using it |
|---|---|---|
| `SINGLE_TRANSACTION` | A SpEL `condition` evaluated against the one incoming transaction | `CTR_THRESHOLD`, `HIGH_RISK_JURISDICTION` |
| `WINDOWED_COUNT` | Count and sum of transactions matching a `filter` within `window-hours` on the same account | `STRUCTURING`, `ROUND_NUMBER` |
| `INFLOW_OUTFLOW_RATIO` | Ratio of outbound to inbound money on an account within `window-hours` | `RAPID_MOVEMENT` |
| `BASELINE_DEVIATION` | Recent activity against the customer's own rolling daily average over `baseline-days` | `BEHAVIORAL_DEVIATION` |

### What each YAML field means

| Field | Meaning |
|---|---|
| `code` | Unique rule identifier, stored on the alert as `alert.rule_code` |
| `typology` | Human label for the AML pattern, stored on the alert and shown to analysts |
| `enabled` | `DetectionEngine` skips the rule entirely when this is `false` |
| `weight` | Base risk score (0-100) the rule contributes when it fires, before customer risk uplifts |
| `type` | One of the four shapes above |
| `scope` | `ACCOUNT` or `CUSTOMER`, naming the entity the time window is computed over |
| `condition` and `filter` | SpEL expressions evaluated against a `Transaction` entity, so you reference its properties by name (`amountBase`, `counterpartyCountry`, `direction`) |
| `threshold` | Meaning depends on `type`: a minimum amount, a minimum matching count, a minimum ratio as a percentage, or a multiplier of the baseline |
| `window-hours` | Lookback window, measured back from the triggering transaction's `txn_timestamp` rather than from wall-clock now |
| `baseline-days` | Historical period used to compute the customer's daily average (`BASELINE_DEVIATION` only) |
| `min-baseline-txns` | Minimum historical transactions required before the engine trusts a baseline (`BASELINE_DEVIATION` only) |
| `explanation` | Template string rendered with rule-specific placeholders (`{amount}`, `{account}`, `{count}`, `{total}`, `{ratio}`, `{window}`, and so on) and stored on the alert |

### A worked example from rules.yml

```yaml
- code: STRUCTURING              # unique id, becomes alert.rule_code
  typology: Structuring / Smurfing
  enabled: true                  # flip to false to disable without deleting
  weight: 60                     # base risk score contribution
  type: WINDOWED_COUNT           # count matching txns in a rolling window
  scope: ACCOUNT                 # window is per-account
  window-hours: 24               # look back 24 hours from the triggering txn
  filter: "amountBase >= 9000 and amountBase <= 9999"   # SpEL over each candidate txn
  threshold: 3                   # need >= 3 matching txns in the window to fire
  explanation: "{count} transactions totalling {total} on {account} within {window} hours, each just below the reporting threshold."
```

Three or more deposits on one account within a day, each of them a little under the $10,000
CTR reporting threshold, is the classic structuring pattern.

**The engine reads the rule book once at startup.** There is no hot reload. After you edit
`rules.yml`, restart the engine. You never need a code change or a rebuild.

### The six rules configured today

| Code | Typology | Weight | Fires when |
|---|---|---|---|
| `CTR_THRESHOLD` | Threshold Reporting | 50 | A single transaction's base-currency amount is >= 10,000 |
| `HIGH_RISK_JURISDICTION` | High-Risk Jurisdiction | 70 | The counterparty country is one of `IR, KP, SY, MM, AF, YE, AE` |
| `STRUCTURING` | Structuring / Smurfing | 60 | 3 or more transactions land on an account within 24h, each between 9,000 and 9,999 |
| `ROUND_NUMBER` | Round-Number Pattern | 40 | 3 or more transactions land on an account within 168h (7 days), each >= 10,000 and an exact multiple of 10,000 |
| `RAPID_MOVEMENT` | Rapid Movement of Funds | 65 | Outflow reaches 80% of inflow on an account within 48h |
| `BEHAVIORAL_DEVIATION` | Behavioural Deviation | 55 | A customer's activity in the last 24h exceeds **10x** their 90-day daily average, given at least 5 historical transactions |

`AlertService` scores every firing. The score starts at the rule's `weight`. It rises by 15
for a politically exposed customer, by 10 for a HIGH risk rating or by 5 for a MEDIUM one,
and by 10 for any KYC status other than VERIFIED. The total is capped at 100. Severity
follows the score: CRITICAL at 85 and above, HIGH at 70, MEDIUM at 50, and LOW below that.

### The database enforces deduplication

Every firing produces a natural `dedup_key`, and `AlertService` updates the existing alert
for that key rather than inserting a second one. The key is built two ways:

- `SINGLE_TRANSACTION` rules use `code:txnRef`, giving one alert per transaction per rule.
- Windowed rules use `code:accountRef-or-customerRef:day`, so a pattern that keeps growing
  through the day folds into one alert whose evidence set and explanation are updated.

`alert.dedup_key` is `UNIQUE` in the schema, so the database enforces deduplication rather
than application locking. That is also why replaying the topic is safe.

## 4. Configuration comes from environment variables

Every variable below has a default suitable for local development.

| Variable | Default | Purpose |
|---|---|---|
| `SENTINEL_DB_URL` | `jdbc:postgresql://localhost:5432/sentinel` | Postgres JDBC URL |
| `SENTINEL_DB_USER` | `catalog` | Postgres user |
| `SENTINEL_DB_PASSWORD` | `catalog` | Postgres password |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker the engine consumes from |
| `SENTINEL_CDC_TOPIC` | `sentinel.public.txn` | Change-event topic. The name must match the connector's `topic.prefix`, then the schema, then the table, so `sentinel` plus `public.txn` gives `sentinel.public.txn`. |

Two consumer settings are fixed in `application.yml` rather than exposed as variables:

- `group-id: sentinel-detection-engine` is the consumer group whose committed offsets decide
  what the engine has already evaluated. Change the group and the engine re-reads the topic
  from the start.
- `auto-offset-reset: earliest` means a brand-new group processes the whole topic, including
  Debezium's initial snapshot, rather than skipping to the tail.

## 5. How to build and run the engine

### What you need first

- Java 21. Check with `java -version`. On this machine `java_home -v 21` can resolve to the
  wrong JDK, so rely on `PATH` rather than overriding `JAVA_HOME`.
- Maven.
- Docker, for Postgres, Kafka, and Kafka Connect.
- `sentinel-aml-service` started at least once against the same database, so that Flyway has
  created the schema. The engine only validates it.
- A registered Debezium connector (section 6).

### Build and run

```bash
mvn clean package
mvn spring-boot:run
# or
java -jar target/sentinel-aml-engine-1.0.0.jar
```

There is no port to hit and no health endpoint. The process starts, joins the consumer
group, and logs `Evaluated transaction TXN-xxxx; alerts=n` for each change event.
`com.tushar.sentinel` logs at `DEBUG`.

To confirm the pipeline works end to end, POST a transaction to the ingestion service and
watch the engine's log:

```bash
curl -u admin:admin -X POST http://localhost:8081/api/v1/transactions \
  -H "Content-Type: application/json" \
  -d '{"txnRef":"TXN-9001","accountRef":"ACC-0001","direction":"DEBIT","txnType":"WIRE",
       "amount":15000,"currency":"USD","counterpartyCountry":"AE",
       "txnTimestamp":"2026-09-19T10:00:00Z"}'
```

## 6. How to set up Debezium

Nothing in this section is wired up by default. Complete all four steps before the engine
can receive a single change event.

### Step 1. Turn on logical decoding in Postgres

**Logical decoding is the main blocker.** It is what lets Debezium read row changes out of
the WAL, and Postgres ships with it turned off. Check the current setting:

```bash
docker exec personal-infra-postgres-1 psql -U catalog -d sentinel -tAc "show wal_level"
```

As of writing, this prints **`replica`**. Logical decoding is **off**, so the connector
cannot start. Its task fails with a message about the WAL level as soon as it tries to
create the replication slot. Change the setting, then restart Postgres. `wal_level` is not
reloadable, and a `SIGHUP` will not pick it up.

You have two ways to change it. The first changes it in place, and the change persists into
the data volume through `postgresql.auto.conf`:

```bash
docker exec personal-infra-postgres-1 psql -U catalog -d sentinel \
  -c "alter system set wal_level = logical"
docker restart personal-infra-postgres-1
docker exec personal-infra-postgres-1 psql -U catalog -d sentinel -tAc "show wal_level"
# must now print: logical
```

The second sets it declaratively on the `postgres` service in the `personal-infra` compose
file. Add a `command:` line, then recreate the container:

```yaml
  postgres:
    image: postgres:16-alpine
    command: ["postgres", "-c", "wal_level=logical"]
    # ...rest unchanged
```

Prefer the declarative version. It survives `docker compose down` and a container recreated
from scratch.

`max_replication_slots` and `max_wal_senders` are both `10` here, which is enough for one
connector. The `catalog` role is a superuser and so already holds `REPLICATION`, so you need
no extra grants. Postgres 16 has `pgoutput` built in, so you install no decoding plugin.

### Step 2. Add a Kafka Connect container

**There is currently no Kafka Connect container.** The `personal-infra` compose project runs
exactly two services: `postgres` (`postgres:16-alpine`, published on 5432) and `kafka`
(`apache/kafka:4.1.1`, KRaft single-node, published on 9092). You have to add Connect
yourself.

Understand one thing before you paste any YAML. The broker is configured with a single
advertised listener:

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
```

A client always reconnects to the *advertised* address, whatever address it bootstrapped
against. That single advertised listener is correct for processes on the host, such as the
engine under `mvn spring-boot:run`. It is wrong for a container. A Connect container that
bootstraps at `kafka:9092` is told the broker lives at `localhost:9092`, and inside that
container `localhost` is the Connect container itself. Connect then cannot reach the broker
and the connector never starts. Give the broker a second listener for clients inside the
Docker network first.

Add these keys to the existing `kafka` service's `environment` and leave the rest unchanged:

```yaml
      KAFKA_LISTENERS: PLAINTEXT://:9092,INTERNAL://:29092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,INTERNAL://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
```

Then add a new service. **Point Connect at `kafka:29092`, the internal listener, not at
`localhost:9092`.**

```yaml
  connect:
    image: quay.io/debezium/connect:3.3.0.Final
    restart: unless-stopped
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
    environment:
      BOOTSTRAP_SERVERS: kafka:29092
      GROUP_ID: sentinel-connect
      CONFIG_STORAGE_TOPIC: connect_configs
      OFFSET_STORAGE_TOPIC: connect_offsets
      STATUS_STORAGE_TOPIC: connect_statuses
      CONFIG_STORAGE_REPLICATION_FACTOR: 1
      OFFSET_STORAGE_REPLICATION_FACTOR: 1
      STATUS_STORAGE_REPLICATION_FACTOR: 1
      KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
    ports:
      - "8083:8083"
```

The worker sets no converter `schemas.enable` on purpose. The connector config sets its own
converters, and per-connector settings override the worker defaults.

The `debezium/connect` image ships the Debezium Postgres connector already, so you install
nothing into the plugin path. Bring Connect up and confirm the REST API answers:

```bash
docker compose up -d connect
curl -s http://localhost:8083/connectors     # expect: []
```

### Step 3. Register the connector

```bash
curl -i -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connector/register-txn-connector.json
```

`connector/register-txn-connector.json` is checked into this repo. It sets the following:

| Key | Value | Why |
|---|---|---|
| `connector.class` | `io.debezium.connector.postgresql.PostgresConnector` | Debezium's Postgres source connector |
| `plugin.name` | `pgoutput` | The logical decoding output plugin built into Postgres 10 and later, so you install nothing |
| `slot.name` | `sentinel_engine` | The replication slot Debezium creates and reads from |
| `publication.name` | `sentinel_engine_pub` | The Postgres publication naming the replicated tables |
| `publication.autocreate.mode` | `filtered` | Creates the publication covering only the tables in `table.include.list`, rather than `FOR ALL TABLES` |
| `topic.prefix` | `sentinel` | Topic names are `<prefix>.<schema>.<table>`, so `txn` lands on `sentinel.public.txn`, matching `SENTINEL_CDC_TOPIC` |
| `table.include.list` | `public.txn` | The transaction table and nothing else. Customers, accounts, and alerts are not streamed. |
| `snapshot.mode` | `initial` | Snapshot the table on first start, then stream from the WAL |
| `decimal.handling.mode` | `string` | Otherwise numerics arrive base64-encoded and you cannot read the change events by eye |
| `key.converter` and `value.converter` | `JsonConverter` with `schemas.enable=false` | The engine tolerates either form, and turning schemas off keeps the change events small and readable |
| `database.hostname` | `localhost` | **Change this to `postgres` when Connect runs in Docker.** Inside the Connect container, `localhost` is Connect itself rather than the database. |

The `database.*` credentials match the ingestion service's defaults: user `catalog`,
password `catalog`, database `sentinel`.

### Step 4. Verify the connector is running

```bash
curl -s http://localhost:8083/connectors/sentinel-txn-connector/status | python3 -m json.tool
```

`connector.state` and every `tasks[].state` must read `RUNNING`. A `FAILED` task carries the
reason in its `trace`. Next, confirm that the slot exists and that the topic is receiving
change events:

```bash
docker exec personal-infra-postgres-1 psql -U catalog -d sentinel \
  -c "select slot_name, plugin, slot_type, active from pg_replication_slots"

docker exec personal-infra-kafka-1 \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic sentinel.public.txn --from-beginning --max-messages 5
```

For the update and delete commands, see [`connector/README.md`](connector/README.md).

## 7. Known gaps

- **Postgres is not configured for logical decoding.** `wal_level` is `replica` on the
  running container, so the connector cannot start at all until you set it to `logical` and
  restart Postgres (section 6, step 1). The `wal_level` setting is the single largest
  obstacle to running the pipeline for real, and it stays invisible until the connector's
  task fails after registration. The `POST /connectors` call itself still returns `201`.
- **A replication slot retains WAL until something consumes it, and deleting the connector
  does not drop the slot.** Delete the connector, pause it, or remove its container while the
  slot lives on, and Postgres keeps every WAL segment since the slot's last confirmed
  position, forever. It will eventually fill the disk, with no obvious symptom until it does.
  Watch the lag with `select slot_name, active,
  pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) from
  pg_replication_slots`. When the connector is gone for good, drop the slot explicitly:

  ```bash
  docker exec personal-infra-postgres-1 psql -U catalog -d sentinel \
    -c "select pg_drop_replication_slot('sentinel_engine')"
  ```

  The same problem has a milder form. On a low-traffic database the slot's confirmed position
  advances only when a change is emitted, so WAL can accumulate because nothing is being
  inserted. Debezium's `heartbeat.interval.ms` exists to fix that, and nobody has configured
  it here.
- **There is no dead-letter topic.** `TransactionChangeListener` catches nothing on purpose.
  A failure rolls the transaction back and Spring Kafka's default error handler takes over.
  That handler retries the record a fixed number of times, then logs it and skips it. The
  offset still advances, so a record that keeps failing is dropped, visible only in the logs,
  with no dead-letter queue to inspect or replay from. Connect's own
  `errors.deadletterqueue.*` settings are not configured either.
- **The initial Debezium snapshot re-emits every existing row.** With `snapshot.mode:
  initial`, registering the connector produces one `op=r` change event per row already in
  `txn`. The current database holds **79 transactions**, so the engine evaluates all 79
  before it sees its first live insert. That is safe, because `dedup_key` is unique and
  re-evaluating a transaction updates the existing alert rather than creating a duplicate.
  It still means a full-table evaluation on first start, and the cost grows linearly with
  table size. Windowed rules make it worse than linear, because each of the 79 change events
  reads its own history window back.
- **The engine assumes nobody updates or deletes a transaction.** `EVALUATED_OPS` is
  `{"c", "r"}`, so inserts and snapshot reads only. The listener logs an `op=u` (update) or
  `op=d` (delete) event at DEBUG and ignores it. Correcting a transaction's amount in the
  database would not re-run detection against it, and deleting a transaction would not
  retract its alert. That matches how the ingestion service behaves today, where
  transactions are append-only, but it is an assumption rather than an enforced constraint.
- **Alerts are scored from the customer's risk profile as it stands at evaluation time.** A
  replay months later would re-score old alerts against the customer's current KYC status and
  risk rating, not the values that applied when the transaction happened.
- **Test coverage is thin.** `src/test` holds one test class,
  `TransactionChangeListenerTest`, whose 7 tests cover the listener's change-event parsing.
  `DetectionEngine` has no tests, so neither the four rule shapes, nor the SpEL evaluation,
  nor the window queries are covered. `AlertService`'s scoring and dedup have no tests
  either. Rule behaviour is only ever exercised against a live database.
