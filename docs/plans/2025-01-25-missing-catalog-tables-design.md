# Missing PostgreSQL Catalog Tables Design

## Overview

Add 7 missing PostgreSQL catalog tables to improve protocol completeness:
- pg_settings
- pg_sequence
- pg_trigger
- pg_collation
- pg_replication_slots
- pg_shdepend
- pg_statistic

## Goals

- **Completeness**: General PostgreSQL protocol completeness
- **Fidelity**: Dynamic where possible, mapping SQLite state where meaningful
- **Approach**: Implement all 7 tables together

## Architecture

Follow the existing **CatalogInterceptor pattern**:
1. Add table names to interception check in `query_interceptor.rs`
2. Create dedicated handler modules for complex tables
3. Handle simpler tables inline in `check_table_factor()`

### Table Classification

| Table | Implementation | Rationale |
|-------|---------------|-----------|
| pg_settings | Dedicated handler | Many rows, commonly queried, needs SHOW/SET integration |
| pg_sequence | Dedicated handler | Must query SQLite's sqlite_sequence table dynamically |
| pg_trigger | Dedicated handler | Must query SQLite's sqlite_master for triggers |
| pg_collation | Inline static | Small fixed set of collations |
| pg_replication_slots | Inline stub | Always empty (SQLite has no replication) |
| pg_shdepend | Inline stub | Always empty (no shared dependencies) |
| pg_statistic | Inline stub | Complex internal format, stub is appropriate |

### File Structure

```
src/catalog/
├── pg_settings.rs    (new)
├── pg_sequence.rs    (new)
├── pg_trigger.rs     (new)
└── mod.rs            (updated)
```

## Detailed Designs

### pg_settings

Exposes PostgreSQL server configuration. Clients query this during connection setup.

**Column schema:**
```
oid, name, setting, unit, category, short_desc, extra_desc,
context, vartype, source, min_val, max_val, enumvals, boot_val,
reset_val, sourcefile, sourceline, pending_restart
```

**Implementation:**
1. Static configuration map with ~30-40 common settings:
   - server_version → "16.0"
   - server_encoding → "UTF8"
   - client_encoding → "UTF8"
   - DateStyle → "ISO, MDY"
   - TimeZone → "UTC"
   - integer_datetimes → "on"
   - standard_conforming_strings → "on"
   - max_connections → "100"

2. Dynamic settings where applicable:
   - search_path → from session state
   - transaction_isolation → current transaction level

3. WHERE clause support for `name` filtering (SHOW command compatibility)

**Handler signature:**
```rust
pub struct PgSettingsHandler;
impl PgSettingsHandler {
    pub async fn handle_query(select: &Select, session: Option<&SessionState>) -> Result<DbResponse, PgSqliteError>;
}
```

### pg_sequence

Maps SQLite's autoincrement sequences to PostgreSQL's sequence catalog.

**Column schema:**
```
seqrelid, seqtypid, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle
```

**Data source:** SQLite's `sqlite_sequence` table:
```sql
SELECT name, seq FROM sqlite_sequence
```

**Mapping:**
- seqrelid → Generate OID from table name hash
- seqtypid → 20 (int8 OID)
- seqstart → 1
- seqincrement → 1
- seqmax → current seq value from sqlite_sequence
- seqmin → 1
- seqcache → 1
- seqcycle → false

### pg_trigger

Maps SQLite triggers to PostgreSQL's trigger catalog.

**Column schema:**
```
oid, tgrelid, tgparentid, tgname, tgfoid, tgtype, tgenabled,
tgisinternal, tgconstrrelid, tgconstrindid, tgconstraint,
tgdeferrable, tginitdeferred, tgnargs, tgattr, tgargs, tgqual,
tgoldtable, tgnewtable
```

**Data source:** SQLite's `sqlite_master`:
```sql
SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger'
```

**Note:** Reuse existing `parse_trigger_sql()` logic from information_schema.triggers implementation.

### pg_collation (Static)

**Column schema:**
```
oid, collname, collnamespace, collowner, collprovider, collisdeterministic,
collencoding, collcollate, collctype, colliculocale, collicurules, collversion
```

**Implementation:** Return 3 standard collations:
- default (oid 100)
- C (oid 950)
- POSIX (oid 951)

### pg_replication_slots (Empty stub)

**Column schema:**
```
slot_name, plugin, slot_type, datoid, database, temporary, active,
active_pid, xmin, catalog_xmin, restart_lsn, confirmed_flush_lsn, ...
```

**Implementation:** Always return empty result set. SQLite has no replication.

### pg_shdepend (Empty stub)

**Column schema:**
```
dbid, classid, objid, objsubid, refclassid, refobjid, deptype
```

**Implementation:** Always return empty result set. Not applicable to SQLite's single-file model.

### pg_statistic (Empty stub)

**Column schema:**
```
starelid, staattnum, stainherit, stanullfrac, stawidth, stadistinct,
stakind1-5, staop1-5, stacoll1-5, stanumbers1-5, stavalues1-5
```

**Implementation:** Always return empty result set. pg_stats (already implemented) is the user-facing view.

## Integration Points

### query_interceptor.rs changes

**1. Update table detection (lines 50-61):**
```rust
let has_catalog_tables = lower_query.contains("pg_catalog") ||
   // ... existing tables ...
   lower_query.contains("pg_settings") ||
   lower_query.contains("pg_sequence") ||
   lower_query.contains("pg_trigger") ||
   lower_query.contains("pg_collation") ||
   lower_query.contains("pg_replication_slots") ||
   lower_query.contains("pg_shdepend") ||
   lower_query.contains("pg_statistic");
```

**2. Add routing in check_table_factor():**
- Route pg_settings, pg_sequence, pg_trigger to dedicated handlers
- Handle pg_collation, pg_replication_slots, pg_shdepend, pg_statistic inline

### mod.rs changes

```rust
pub mod pg_settings;
pub mod pg_sequence;
pub mod pg_trigger;
```

## Migration Consideration

No SQLite migrations needed - these are all virtual tables handled by the interceptor, not backed by SQLite tables.

## Estimates

| Table | Type | Lines of code |
|-------|------|---------------|
| pg_settings | Dedicated handler | ~200 |
| pg_sequence | Dedicated handler | ~100 |
| pg_trigger | Dedicated handler | ~120 |
| pg_collation | Inline static | ~40 |
| pg_replication_slots | Inline stub | ~20 |
| pg_shdepend | Inline stub | ~20 |
| pg_statistic | Inline stub | ~30 |

**Total:** ~530 lines of new code
