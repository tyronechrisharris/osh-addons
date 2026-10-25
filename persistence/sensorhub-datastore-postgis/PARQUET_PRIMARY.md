# Parquet-primary observation mode

`OscarTieredObsSystemDatabase` supports an explicit `PARQUET_PRIMARY` mode for
fresh OSCAR databases. Ordinary PostGIS stores and the default
`HOT_RETENTION` tiered mode are unchanged.

## Storage contract

- PostgreSQL is the durable staging and rollback tier. A write becomes durable
  when the normal OSH store `commit()` completes; `add()` alone is not a
  durability boundary because the batch store intentionally buffers writes.
- Only output names in `archive.outputNames` are eligible. In primary mode this
  list must contain only streams that are immutable once committed. Mutable or
  open occupancy, alarm, and adjudication workflow state must remain in
  PostgreSQL.
- Completed UTC-hour batches become eligible after
  `parquetPrimaryArchiveDelayMinutes`. Row and byte caps bound each generation;
  a large hour is exported as deterministic revisions over later cycles.
- Export continues to use a transactionally claimed job, `.partial` files,
  file and directory sync, atomic rename, checksum/count/range validation, an
  exact activation ledger, and manifest-last publication. A restart reclaims
  stale jobs and reuses deterministic generation object names.
- PostgreSQL rows are deleted only after activation, parity verification,
  `rollbackGraceDays`, retention-hold checks, and the existing bounded deletion
  lifecycle. `deleteHotAfterArchive=true` is therefore mandatory in primary
  mode.
- Primary mode verifies up to `maximumVerificationsPerCycle` older generations
  before exporting the next bounded batch. Configuration validation requires
  that cap to be at least `maximumGenerationsPerCycle`, preventing each cycle
  from creating parity work faster than a later cycle can process it. A newly
  activated generation is intentionally first eligible for verification on
  the next scheduled cycle. The verification cap is separate because parity
  rereads both Parquet and PostgreSQL and is not covered by the export deadline.
- Hot deletion has its own `maximumDeletionsPerCycle` bound. Primary-mode
  validation likewise requires it to be at least the export-generation cap so
  eligible staging rows cannot accumulate solely because of a fixed lifecycle
  rate mismatch. Verification and deletion still run before the export
  deadline begins, so configure all three caps for the host's measured I/O
  budget.
- Mixed hot/cold queries, mutation overlays, tombstones, stable observation
  IDs, and late-arrival revisions continue to use the existing tiered reader.
  A single-ID lookup first prunes the catalog through
  `oscar_archive_id_block`, then applies Parquet predicate pushdown; it does not
  scan every generation.

`rollbackGraceDays=3` is a minimum three-day rollback overlap measured from
generation activation, not a guaranteed 72-hour deletion deadline measured
from observation arrival. Backlog, a failed parity receipt (which stops that
lifecycle pass), an active hold, an unavailable archive, or a stopped scheduler
safely delays deletion. Operations must alert on unverified-generation and
unarchived-row backlog if a bounded storage envelope is required. PostgreSQL
normally reuses space freed by deletion; the volume's physical file size need
not fall immediately and still depends on autovacuum/checkpoint operation.

OSCAR's public event identifier is decoded to the stable OSH observation ID
before this store is queried. The direct cold lookup then uses
`oscar_archive_id_block`; this slice does not claim that the currently unused
`oscar_event_locator` table provides UUID indexing.

## Index policy

On a fresh primary-mode observation table, OSCAR omits the redundant generic
`GIN(result)` index, which was the largest payload index on the measured test
system. The purpose-built `jsonb_path_ops` and numeric event-filter indexes
remain because mutable occupancy, alarm, and adjudication workflow rows are
still PostgreSQL-authoritative and must remain searchable. Index entries for
allowlisted raw streams age out with bounded hot deletion and can reuse space;
entries for retained non-allowlisted workflow data remain and can still grow.
That residual growth must be monitored rather than described as eliminated.
Narrow identity, natural-key, stream, time, FOI, unarchived-time, and generation
indexes also remain for ingestion, idempotency, staging scans, lifecycle work,
and hot queries.

This mode does not drop indexes from an existing database. That deliberate
fresh-schema rule prevents a configuration change from causing destructive DDL
or surprising an upgraded installation.

Eliminating every PostgreSQL payload index is intentionally out of scope for
this slice. It first requires a reliable finalized-event transition and hot
query predicates that exclude archive-owned overlap rows; removing those
indexes before that work would trade storage savings for progressively slower
alarm searches.

Parquet supplies column statistics and DuckDB predicate pushdown for the typed
ID, datastream, FOI, and time columns. Arbitrary predicates inside the retained
JSON result still scan the already-pruned candidate generations; this change
does not claim a Parquet secondary or Bloom index that the checked dependency
does not provide.

## Example

```json
{
  "archive": {
    "enabled": true,
    "storageMode": "PARQUET_PRIMARY",
    "parquetPrimaryArchiveDelayMinutes": 5,
    "intervalMinutes": 5,
    "maximumGenerationsPerCycle": 2,
    "maximumVerificationsPerCycle": 2,
    "maximumDeletionsPerCycle": 2,
    "deleteHotAfterArchive": true,
    "rollbackGraceDays": 3,
    "outputNames": [
      "gammaCounts",
      "gammaThreshold",
      "neutronCounts",
      "setupGamma",
      "setupNeutron"
    ]
  }
}
```

Do not describe this mode as direct-to-Parquet ingestion: PostgreSQL staging is
intentional. It provides a crash-recoverable commit boundary while Parquet
generation remains asynchronous and bounded.

Before enabling this profile in a release, run the disposable real-PostgreSQL
integration suite against a fresh schema and verify the actual index catalog,
advanced hot event filters, archive/parity/deletion, restart recovery, and
single-ID hot/cold lookup. Unit query-builder tests do not replace that gate.
