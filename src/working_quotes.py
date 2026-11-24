"""Normalized working quote storage, compatible with existing public SQL.

Only the disposable Actions database uses this layout; the existing canonical
checkpoint and HF Raw format do not change. UUIDs, batch timestamps and
identical monetary tuples are stored once. The view preserves all columns.
"""
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS work_quote_skus(
  id INTEGER PRIMARY KEY, store_uuid TEXT NOT NULL, product_uuid TEXT NOT NULL,
  UNIQUE(store_uuid,product_uuid));
CREATE TABLE IF NOT EXISTS work_quote_batches(
  id INTEGER PRIMARY KEY, batch_id TEXT NOT NULL UNIQUE, observed_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS work_quote_values(
  id INTEGER PRIMARY KEY, menu_price_cents INTEGER NOT NULL,
  promotion_type TEXT NOT NULL CHECK(promotion_type IN ('NONE','SAME_SKU_BXGY','DIRECT_DISCOUNT')),
  buy_quantity INTEGER NOT NULL, free_quantity INTEGER NOT NULL,
  received_quantity INTEGER NOT NULL, original_total_cents INTEGER NOT NULL,
  payable_total_cents INTEGER NOT NULL,
  UNIQUE(menu_price_cents,promotion_type,buy_quantity,free_quantity,received_quantity,
         original_total_cents,payable_total_cents));
CREATE TABLE IF NOT EXISTS work_quote_observations(
  sku INTEGER NOT NULL REFERENCES work_quote_skus(id),
  batch INTEGER NOT NULL REFERENCES work_quote_batches(id),
  quote INTEGER NOT NULL REFERENCES work_quote_values(id),
  source_hash TEXT NOT NULL,
  PRIMARY KEY(sku,batch)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS work_quote_by_batch ON work_quote_observations(batch,sku);
CREATE VIEW IF NOT EXISTS product_price_observations AS
SELECT b.batch_id,s.store_uuid,s.product_uuid,b.observed_at,
       v.menu_price_cents,v.promotion_type,v.buy_quantity,v.free_quantity,
       v.received_quantity,v.original_total_cents,v.payable_total_cents,o.source_hash
FROM work_quote_skus s JOIN work_quote_observations o ON o.sku=s.id
JOIN work_quote_batches b ON b.id=o.batch
JOIN work_quote_values v ON v.id=o.quote;
CREATE TRIGGER IF NOT EXISTS work_quote_insert INSTEAD OF INSERT ON product_price_observations
BEGIN
  INSERT INTO work_quote_skus(store_uuid,product_uuid)
    SELECT NEW.store_uuid,NEW.product_uuid
    WHERE NOT EXISTS(SELECT 1 FROM work_quote_skus
      WHERE store_uuid=NEW.store_uuid AND product_uuid=NEW.product_uuid);
  INSERT INTO work_quote_batches(batch_id,observed_at)
    SELECT NEW.batch_id,NEW.observed_at
    WHERE NOT EXISTS(SELECT 1 FROM work_quote_batches WHERE batch_id=NEW.batch_id);
  SELECT CASE WHEN EXISTS(SELECT 1 FROM work_quote_batches
     WHERE batch_id=NEW.batch_id AND observed_at<>NEW.observed_at)
     THEN RAISE(ABORT,'inconsistent observation timestamp') END;
  INSERT INTO work_quote_values(menu_price_cents,promotion_type,buy_quantity,free_quantity,
    received_quantity,original_total_cents,payable_total_cents)
    SELECT NEW.menu_price_cents,NEW.promotion_type,NEW.buy_quantity,NEW.free_quantity,
           NEW.received_quantity,NEW.original_total_cents,NEW.payable_total_cents
    WHERE NOT EXISTS(SELECT 1 FROM work_quote_values WHERE
      menu_price_cents=NEW.menu_price_cents AND promotion_type=NEW.promotion_type
      AND buy_quantity=NEW.buy_quantity AND free_quantity=NEW.free_quantity
      AND received_quantity=NEW.received_quantity AND original_total_cents=NEW.original_total_cents
      AND payable_total_cents=NEW.payable_total_cents);
  INSERT INTO work_quote_observations(sku,batch,quote,source_hash)
    SELECT s.id,b.id,v.id,NEW.source_hash
    FROM work_quote_skus s,work_quote_batches b,work_quote_values v
    WHERE s.store_uuid=NEW.store_uuid AND s.product_uuid=NEW.product_uuid
      AND b.batch_id=NEW.batch_id
      AND v.menu_price_cents=NEW.menu_price_cents AND v.promotion_type=NEW.promotion_type
      AND v.buy_quantity=NEW.buy_quantity AND v.free_quantity=NEW.free_quantity
      AND v.received_quantity=NEW.received_quantity AND v.original_total_cents=NEW.original_total_cents
      AND v.payable_total_cents=NEW.payable_total_cents
    ON CONFLICT(sku,batch) DO UPDATE SET quote=excluded.quote,source_hash=excluded.source_hash;
END;
CREATE TRIGGER IF NOT EXISTS work_quote_delete INSTEAD OF DELETE ON product_price_observations
BEGIN
 DELETE FROM work_quote_observations WHERE
 sku=(SELECT id FROM work_quote_skus WHERE store_uuid=OLD.store_uuid AND product_uuid=OLD.product_uuid)
 AND batch=(SELECT id FROM work_quote_batches WHERE batch_id=OLD.batch_id);
END;
"""

def is_normalized(conn):
    return conn.execute("SELECT type FROM sqlite_master WHERE name='product_price_observations'").fetchone() == ("view",) or (
        (row := conn.execute("SELECT type FROM sqlite_master WHERE name='product_price_observations'").fetchone())
        is not None and row[0] == "view")

def initialize(conn):
    old=conn.execute("SELECT type FROM sqlite_master WHERE name='product_price_observations'").fetchone()
    if old and old[0] != "view":
        raise ValueError("Cannot overlay a normalized quote view on a legacy table")
    conn.executescript(SCHEMA)

class BulkQuoteWriter:
    """Use integer keys directly during verified checkpoint expansion.

    Product identity cache is bounded to one store; the batch/value caches
    are compact and independent of the number of historical observations.
    """
    def __init__(self,conn):
        self.conn=conn
        self.batch={}
        self.values={}
        self.store=None
        self.skus={}
    def write(self,row):
        batch,sid,pid,at,*tail=row
        value=tuple(tail[:-1])
        source_hash=tail[-1]
        if sid!=self.store:
            self.skus={}
            self.store=sid
        if pid not in self.skus:
            self.conn.execute("INSERT OR IGNORE INTO work_quote_skus(store_uuid,product_uuid) VALUES(?,?)",(sid,pid))
            self.skus[pid]=self.conn.execute("SELECT id FROM work_quote_skus WHERE store_uuid=? AND product_uuid=?",(sid,pid)).fetchone()[0]
        if batch not in self.batch:
            self.conn.execute("INSERT OR IGNORE INTO work_quote_batches(batch_id,observed_at) VALUES(?,?)",(batch,at))
            found=self.conn.execute("SELECT id,observed_at FROM work_quote_batches WHERE batch_id=?",(batch,)).fetchone()
            self.batch[batch]=(found[0],found[1])
        batch_key,stored_at=self.batch[batch]
        if at!=stored_at:
            raise ValueError("inconsistent observation timestamp")
        if value not in self.values:
            self.conn.execute("""INSERT OR IGNORE INTO work_quote_values(
              menu_price_cents,promotion_type,buy_quantity,free_quantity,received_quantity,
              original_total_cents,payable_total_cents) VALUES(?,?,?,?,?,?,?)""",value)
            found=self.conn.execute("""SELECT id FROM work_quote_values WHERE
             menu_price_cents=? AND promotion_type=? AND buy_quantity=? AND free_quantity=?
             AND received_quantity=? AND original_total_cents=? AND payable_total_cents=?""",value).fetchone()
            if found is None:
                raise ValueError("invalid quote tuple")
            if len(self.values)>=65536:
                self.values.clear()
            self.values[value]=found[0]
        self.conn.execute("INSERT INTO work_quote_observations VALUES(?,?,?,?)",
                          (self.skus[pid],batch_key,self.values[value],source_hash))
