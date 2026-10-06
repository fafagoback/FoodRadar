"""Publish only changed packed rows in one transaction to FoodRadar's own DB."""
import argparse
import base64
import json
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


def remote_rows(remote, table, exists):
    if table not in exists:
        return {}
    cols = ','.join((*TABLE_KEYS[table], 'checksum')) if table.endswith('bundles') or table == 'search_buckets' else '*'
    rows = remote.query(f'SELECT {cols} FROM {table}')
    return {tuple(row[k] for k in TABLE_KEYS[table]): row for row in rows}


def plan_table(table, local_rows, published):
    keys = TABLE_KEYS[table]
    candidate = {tuple(row[k] for k in keys): row for row in local_rows}
    changed = [row for key, row in candidate.items()
               if key not in published or fingerprint(row) != fingerprint(published[key])]
    deleted = [key for key in published if key not in candidate]
    return changed, deleted


def publish(database, remote, apply=False):
    conn = sqlite3.connect(f'{Path(database).resolve().as_uri()}?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    try:
        local_meta = dict(conn.execute('SELECT key,value FROM metadata'))
        if json.loads(local_meta.get('application', 'null')) != 'FoodRadar':
            raise ValueError('Candidate is not a FoodRadar web database')
        tables = {r['name'] for r in remote.query("SELECT name FROM sqlite_master WHERE type='table'")}
        old_meta = {r['key']: r['value'] for r in remote.query('SELECT key,value FROM metadata')} if 'metadata' in tables else {}
        if tables and json.loads(old_meta.get('application', 'null')) != 'FoodRadar':
            raise ValueError('Refusing an existing database owned by another application')
        if old_meta and old_meta.get('schema_version') != local_meta['schema_version']:
            raise ValueError('Schema changes require a new versioned database')
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
            plans[table] = changed, deleted
            write_bytes += sum(len(json.dumps([cell(v) for v in row.values()]).encode()) for row in changed)
            stats[table] = {'upsert': len(changed), 'delete': len(deleted), 'total': len(local)}
        report = {'previous_batch': previous_batch, 'latest_batch': target_batch,
                  'initialization': not bool(old_meta), 'delta_bytes': write_bytes,
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
                for key in deleted:
                    pending.append((f"DELETE FROM {table} WHERE {' AND '.join(k+'=?' for k in keys)}", key, False))
                    if len(pending) >= 50:
                        flush()
            flush()
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
    parser.add_argument('--report', default='delta-report.json')
    args = parser.parse_args()
    remote = Remote(os.environ['TURSO_DATABASE_URL'], os.environ['TURSO_AUTH_TOKEN'])
    try:
        report = publish(args.database, remote, args.apply)
        Path(args.report).write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(json.dumps(report))
    finally:
        remote.close()
