"""Publish only changed packed rows in one transaction to FoodRadar's own DB."""
import argparse
import base64
import json
import math
import os
import sqlite3
from pathlib import Path
from urllib.parse import urlparse

import requests

TABLE_KEYS = {
    'store_directory': ('store_id',),
    'store_bundles': ('store_id', 'chunk_no'),
    'search_buckets': ('bucket_id',),
    'auxiliary_bundles': ('name',),
    'metadata': ('key',),
    'web_products': ('store_uuid', 'product_uuid'),
}
MAX_DELTA_BYTES = 262_144_000


def cell(value):
    if value is None:
        return {'type': 'null'}
    if isinstance(value, bytes):
        return {'type': 'blob', 'base64': base64.b64encode(value).decode()}
    if isinstance(value, int):
        return {'type': 'integer', 'value': str(value)}
    if isinstance(value, float):
        return {'type': 'float', 'value': value}
    return {'type': 'text', 'value': value}


class Remote:
    def __init__(self, url, token):
        host = urlparse(url.replace('libsql://', 'https://')).hostname or ''
        if not host.startswith('foodradar-') or not host.endswith('.turso.io'):
            raise ValueError('Only an independent foodradar-* Turso database is allowed')
        self.url = f'https://{host}/v2/pipeline'
        self.session = requests.Session()
        self.session.headers.update(Authorization=f'Bearer {token}')
        self.baton = None

    def execute(self, statements):
        # No retry for writes: an ambiguous commit is resolved by rerunning the
        # checksum plan against remote state, never by blindly replaying SQL.
        body = {'requests': [{'type': 'execute', 'stmt': {
            'sql': sql, 'args': [cell(v) for v in args], 'want_rows': read
        }} for sql, args, read in statements]}
        if self.baton:
            body['baton'] = self.baton
        response = self.session.post(self.url, json=body, timeout=90)
        response.raise_for_status()
        result = response.json()
        self.baton = result.get('baton')
        rows = []
        for item in result['results']:
            if item['type'] == 'error':
                raise RuntimeError(item['error'].get('message', 'Turso statement failed'))
            value = item['response']['result']
            names = [c['name'] for c in value['cols']]
            rows.append([dict(zip(names, [None if c['type'] == 'null' else
                int(c['value']) if c['type'] == 'integer' else
                float(c['value']) if c['type'] == 'float' else c.get('value')
                for c in row])) for row in value['rows']])
        return rows

    def query(self, sql, args=()):
        return self.execute([(sql, args, True)])[0]

    def close(self):
        if self.baton:
            self.session.post(self.url, json={'baton': self.baton,
                              'requests': [{'type': 'close'}]}, timeout=30)
        self.session.close()


def fingerprint(row):
    # Never download remote blobs merely to decide whether to upload them.
    return row['checksum'] if 'checksum' in row else tuple(sorted(row.items()))


def same_row(candidate, published):
    if 'checksum' in candidate:
        return candidate['checksum'] == published.get('checksum')
    if set(candidate) != set(published):
        return False
    for key,value in candidate.items():
        other = published[key]
        # The HTTP server's float formatting can shift a coordinate by one ULP.
        # This tolerance is far below a cent or any meaningful GPS difference.
        if isinstance(value,float) and isinstance(other,(int,float)):
            if not math.isclose(value,other,rel_tol=1e-14,abs_tol=1e-12):
                return False
        elif value != other:
            return False
    return True


def remote_rows(remote, table, exists):
    if table not in exists:
        return {}
    cols = ','.join((*TABLE_KEYS[table], 'checksum')) if table.endswith('bundles') or table in ('search_buckets','web_products') else '*'
    rows = remote.query(f'SELECT {cols} FROM {table}')
    return {tuple(row[k] for k in TABLE_KEYS[table]): row for row in rows}


def plan_table(table, local_rows, published):
    keys = TABLE_KEYS[table]
    candidate = {tuple(row[k] for k in keys): row for row in local_rows}
    changed = [row for key, row in candidate.items()
               if key not in published or not same_row(row,published[key])]
    deleted = [key for key in published if key not in candidate]
    if table == 'store_directory':
        # Packed store_id values are positional and may be re-numbered after
        # repairing Raw history. A UUID can move to another store_id even when
        # both IDs are still present: delete its old slot before any upsert.
        old_ids_by_uuid = {row['store_uuid']: key for key,row in published.items()
                           if row.get('store_uuid') is not None}
        for key,row in candidate.items():
            former = old_ids_by_uuid.get(row.get('store_uuid'))
            if former is not None and former != key and former not in deleted:
                deleted.append(former)
    return changed, deleted


def verified_rebuild_evidence(path, local_meta, old_meta):
    if not path:
        return False
    try:
        proof = json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError,ValueError) as exc:
        raise ValueError('Full Raw replay proof missing or invalid') from exc
    local_counts = json.loads(local_meta.get('source_counts','{}'))
    previous_counts = json.loads(old_meta.get('source_counts','{}'))
    required_count = int(local_counts.get('crawl_batches',0))
    verified_count = proof.get('verified_manifest_count')
    if proof.get('format') == 'foodradar-immutable-raw-only-rebuild-v1':
        # An explicitly requested reset MUST discard the rejected historical
        # dataset, so comparing to prior (dirty) history counts is incorrect.
        # The source has independently been SHA256 checked and replayed from
        # FoodRadar/Raw alone. Never allow legacy paths as reset evidence.
        if (proof.get('batch_id') != json.loads(local_meta.get('latest_batch', 'null'))
                or proof.get('source_prefix') != 'Raw/'
                or proof.get('legacy_snapshot_count') != 0
                or not isinstance(verified_count, int) or verified_count < 1
                or proof.get('raw_manifest_count') != verified_count
                or verified_count != required_count
                or proof.get('source_counts', {}).get('stores')
                    != int(local_counts.get('stores', -1))
                or proof.get('source_counts', {}).get('products')
                    != int(local_counts.get('products', -1))
                or not isinstance(proof.get('manifest_digest'), str)
                or len(proof['manifest_digest']) != 64):
            raise ValueError('Raw-only reset evidence does not match candidate')
        return True
    if (proof.get('format') != 'foodradar-full-raw-replay-v2'
        or proof.get('batch_id') != json.loads(local_meta.get('latest_batch','null'))
        or not isinstance(verified_count,int) or verified_count < 1
        or verified_count != required_count
        or verified_count < int(previous_counts.get('crawl_batches',0))
        or proof.get('source_counts',{}).get('stores') != int(local_counts.get('stores',-1))
        or proof.get('source_counts',{}).get('products') != int(local_counts.get('products',-1))
        or not isinstance(proof.get('manifest_digest'),str)
        or len(proof['manifest_digest']) != 64
    ):
        raise ValueError('Full Raw replay proof does not match packed release or prior archive coverage')
    return True


def publish(database, remote, apply=False, verified_rebuild_manifest=None, reset_all=False):
    conn = sqlite3.connect(f'{Path(database).resolve().as_uri()}?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    try:
        local_meta = dict(conn.execute('SELECT key,value FROM metadata'))
        if json.loads(local_meta.get('application', 'null')) != 'FoodRadar':
            raise ValueError('Candidate is not a FoodRadar web database')
        tables = {r['name'] for r in remote.query("SELECT name FROM sqlite_master WHERE type='table'")}
        old_meta = {r['key']: r['value'] for r in remote.query('SELECT key,value FROM metadata')} if 'metadata' in tables else {}
        if tables and json.loads(old_meta.get('application', 'null')) != 'FoodRadar':
            # A wiped FoodRadar database may retain its six empty table schemas.
            # This is safe to initialize ONLY when the exact expected tables
            # exist and every one is empty. Do not infer ownership of any
            # populated, partly reset, or unknown application database.
            row_counts = {
                table: int(remote.query(f'SELECT COUNT(*) AS count FROM {table}')[0]['count'])
                for table in sorted(tables)
            }
            empty_foodradar_schema = (
                not old_meta
                and tables == set(TABLE_KEYS)
                and all(count == 0 for count in row_counts.values())
            )
            if not empty_foodradar_schema:
                safe_keys = ('application', 'definition_version', 'schema_version', 'latest_batch')
                diagnostics = {k: old_meta.get(k) for k in safe_keys}
                raise ValueError('Refusing an unidentified or partially populated Turso database: '
                                 + json.dumps({'tables': sorted(tables),
                                               'row_counts': row_counts,
                                               'metadata': diagnostics}, ensure_ascii=False))
            print('FOODRADAR_EMPTY_SCHEMA_INIT=' + json.dumps(
                {'tables': sorted(tables), 'row_counts': row_counts}, ensure_ascii=False), flush=True)
        trusted_rebuild = verified_rebuild_evidence(
            verified_rebuild_manifest, local_meta, old_meta)
        if old_meta and old_meta.get('schema_version') != local_meta['schema_version']:
            if not (reset_all and trusted_rebuild
                    and json.loads(local_meta['schema_version']) == 2):
                raise ValueError('Schema changes require verified V2 rebuild/reset')
        if reset_all and not trusted_rebuild:
            raise ValueError('Refusing to erase Turso without full checksum-verified HF RAW replay')
        # A historical Raw archive can be incomplete even when its checksum is
        # valid. Never turn a re-pack into a destructive mass purge of known
        # stores or products without a separately audited source of truth.
        if 'store_directory' in tables:
            remote_stores = int(remote.query('SELECT COUNT(*) AS count FROM store_directory')[0]['count'])
            local_stores = int(conn.execute('SELECT COUNT(*) FROM store_directory').fetchone()[0])
            if remote_stores >= 100 and local_stores < remote_stores * 0.7 and not trusted_rebuild:
                raise ValueError(
                    f'Unsafe store shrink: {remote_stores} -> {local_stores}; '
                    'HF Raw replay may omit historical snapshots. Refusing publish.')
        if old_meta:
            before_counts = json.loads(old_meta.get('source_counts', '{}'))
            after_counts = json.loads(local_meta.get('source_counts', '{}'))
            for kind in ('stores','products'):
                before = int(before_counts.get(kind, 0))
                after = int(after_counts.get(kind, 0))
                if before >= 100 and after < before * 0.7 and not trusted_rebuild:
                    raise ValueError(
                        f'Unsafe {kind} shrink: {before} -> {after}; '
                        'do not replace a durable checkpoint with incomplete HF Raw.')
        target_batch = json.loads(local_meta['latest_batch'])
        previous_batch = json.loads(old_meta.get('latest_batch', '""'))
        if previous_batch > target_batch:
            raise ValueError('Refusing an older release')
        plans = {}
        stats = {}
        write_bytes = 0
        for table in TABLE_KEYS:
            local = [dict(row) for row in conn.execute(f'SELECT * FROM {table}')]
            changed, deleted = plan_table(table, local, remote_rows(remote, table, tables))
            if reset_all:
                # Explicitly discard every previous Turso row, then load only
                # the freshly rebuilt HF source. No backup is created.
                changed = local
                deleted = []
            plans[table] = changed, deleted
            write_bytes += sum(len(json.dumps([cell(v) for v in row.values()]).encode()) for row in changed)
            stats[table] = {'upsert': len(changed), 'delete': len(deleted), 'total': len(local)}
        report = {'previous_batch': previous_batch, 'latest_batch': target_batch,
                  'initialization': not bool(old_meta),
                  'verified_full_raw_rebuild': trusted_rebuild, 'reset_all': reset_all,
                  'delta_bytes': write_bytes,
                  'tables': stats, 'applied': False}
        if write_bytes > MAX_DELTA_BYTES:
            raise ValueError('Daily delta exceeds 250 MiB; refusing full replacement')
        if not apply or not any(a or b for a,b in plans.values()):
            return report
        remote.query('BEGIN IMMEDIATE')
        try:
            # Recheck under the write lock; a concurrently published batch must
            # not be overwritten using a stale delta plan.
            if 'metadata' in tables:
                locked = {r['key']: r['value'] for r in remote.query('SELECT key,value FROM metadata')}
                if locked != old_meta:
                    raise ValueError('Remote release changed during planning')
            for table in TABLE_KEYS:
                ddl = conn.execute('SELECT sql FROM sqlite_master WHERE type=? AND name=?', ('table',table)).fetchone()[0]
                if table not in tables:
                    remote.query(ddl)
            pending = []
            size = 0
            def flush():
                nonlocal size
                if pending:
                    remote.execute(pending)
                    pending.clear()
                    size = 0
            for table, (changed, deleted) in plans.items():
                keys = TABLE_KEYS[table]
                if reset_all:
                    flush()
                    remote.query(f'DELETE FROM {table}')
                # Delete first, then upsert. Upserting a renumbered store UUID
                # before freeing its old store_id hits UNIQUE(store_uuid).
                # This all runs inside the same transaction, so a failure
                # rolls back both deletion and insertion.
                for key in deleted:
                    pending.append((f"DELETE FROM {table} WHERE {' AND '.join(k+'=?' for k in keys)}", key, False))
                    if len(pending) >= 50:
                        flush()
                flush()
                for row in changed:
                    cols = list(row)
                    updates = ','.join(f'{c}=excluded.{c}' for c in cols if c not in keys)
                    sql = f"INSERT INTO {table}({','.join(cols)}) VALUES({','.join('?' for _ in cols)}) ON CONFLICT({','.join(keys)}) DO UPDATE SET {updates}"
                    args = tuple(row.values())
                    item_size = len(json.dumps([cell(v) for v in args]).encode())
                    if pending and (size + item_size > 750_000 or len(pending) >= 50):
                        flush()
                    pending.append((sql,args,False))
                    size += item_size
            flush()
            # Create optimized indexes after the bulk upsert, inside the same transaction.
            for (ddl,) in conn.execute("SELECT sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL AND tbl_name='web_products'"):
                # Incremental Turso publishes preserve indexes from prior runs.
                # Recreating them without IF NOT EXISTS makes every subsequent
                # publication fail with "index idx_web_deals already exists".
                if not ddl.startswith('CREATE INDEX '):
                    raise ValueError('Unexpected packed web-products index DDL')
                remote.query(ddl.replace('CREATE INDEX ', 'CREATE INDEX IF NOT EXISTS ', 1))
            # Verify every key/checksum and directory/metadata value before commit.
            for table in TABLE_KEYS:
                local = [dict(row) for row in conn.execute(f'SELECT * FROM {table}')]
                changed, deleted = plan_table(table, local, remote_rows(remote,table,set(TABLE_KEYS)))
                if changed or deleted:
                    details = ''
                    if changed and table == 'store_directory':
                        row = changed[0]
                        old = remote_rows(remote,table,set(TABLE_KEYS)).get(tuple(row[k] for k in TABLE_KEYS[table]),{})
                        details = str({key: {'expected':value,'received':old.get(key)} for key,value in row.items() if value != old.get(key)})
                    raise ValueError(f'Remote verification failed: {table}; {details}')
            remote.query('COMMIT')
            report['applied'] = True
            return report
        except BaseException:
            try:
                remote.query('ROLLBACK')
            except Exception:
                pass
            raise
    finally:
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('database')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--verified-rebuild-manifest',default=None)
    parser.add_argument('--reset-all', action='store_true',
                        help='Erase every FoodRadar Turso table within the same transaction before inserting full verified HF RAW rebuild')
    parser.add_argument('--report', default='delta-report.json')
    args = parser.parse_args()
    remote = Remote(os.environ['TURSO_DATABASE_URL'], os.environ['TURSO_AUTH_TOKEN'])
    try:
        report = publish(args.database, remote, args.apply, args.verified_rebuild_manifest, args.reset_all)
        Path(args.report).write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(json.dumps(report))
    finally:
        remote.close()
