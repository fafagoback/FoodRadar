"""Recover old HF menu *quotes* separately from verified payable prices.

Historical getStoreV1 calls before the location-cookie fix often contained
a priced menu but an explicit 'Delivery unavailable' banner. The prices are
real archived menu QUOTES, not verified customer-payable prices. We only
surface independently repeated, scheduled-open menu quotes as suggestions;
they must never alter the main verified seven-day comparison.
"""
import collections
from datetime import datetime, timedelta, timezone
import hashlib
import html
import json
import os
from pathlib import Path
import sqlite3
import tarfile

from json_to_db import calculate_effective_price, extract_promo_info
from price_observation import _schedule_open, observed_at, price_observation_valid
from price_change import PriceChange
from serving_state import ensure_schema_and_migrations, product_identity, store_identity

TW=timezone(timedelta(hours=8))
OLD_BANNER='delivery unavailable'
BAD_TAGLINES=('opens ','opening ','closed','temporarily closed',
              'currently closed','未營業','尚未營業','休息中','已打烊','暫停營業')

def archival_quote_eligible(doc, batch):
    """Only pre-fix unlocated menus, open store flag + explicit open schedule."""
    return (
        doc.get('isOpen') is True
        and doc.get('menu_status') == 'present'
        and str(doc.get('closedMessage') or '').strip().lower() == OLD_BANNER
        and not str(doc.get('workingHoursTagline') or '').strip().lower().startswith(BAD_TAGLINES)
        and _schedule_open(doc, observed_at(doc,batch)) is True
        and not price_observation_valid(doc,batch)
    )

def canonical_menu_quotes(doc, sid):
    """Resolve duplicate promo categories before extracting one quote per SKU."""
    sections=(doc.get('hasMenu') or {}).get('hasMenuSection') or []
    preferred={}
    for section in sections:
        category=html.unescape(str(section.get('name') or '一般'))
        for item in section.get('hasMenuItem') or []:
            name=html.unescape(str(item.get('name') or '')).strip()
            if not name:continue
            pid,_=product_identity(item,sid)
            inferred,qty=extract_promo_info(category,name,str(item.get('description') or ''))
            promo=str(item.get('promo_type') or inferred)
            try:qty=max(1,int(item.get('quantity') or qty))
            except (TypeError,ValueError):qty=1
            if promo=='無':continue
            rank=(int(qty>1),promo)
            if pid not in preferred or rank>preferred[pid][0]:
                preferred[pid]=(rank,promo,qty)
    results={}
    encountered_raw_prices=collections.defaultdict(set)
    for section in sections:
        category=html.unescape(str(section.get('name') or '一般'))
        for item in section.get('hasMenuItem') or []:
            name=html.unescape(str(item.get('name') or '')).strip()
            if not name:continue
            pid,_=product_identity(item,sid)
            if pid in results:continue
            try:raw=float((item.get('offers') or {}).get('price') or 0)
            except (ValueError,TypeError):continue
            if not 0<raw<1000000:continue
            # A SKU can be featured in several sections. If the actual offer
            # prices disagree, the archive's list order cannot establish which
            # price was payable. Exclude ambiguous quotes rather than invent
            # price drops based on whichever category appears first.
            encountered_raw_prices[pid].add(round(raw,2))
            inferred,qty=extract_promo_info(category,name,str(item.get('description') or ''))
            promo=str(item.get('promo_type') or inferred)
            try:qty=max(1,int(item.get('quantity') or qty))
            except (ValueError,TypeError):qty=1
            if pid in preferred:
                _,promo,qty=preferred[pid]
            unit=calculate_effective_price(raw,promo,qty)
            if 0<unit<1000000:results[pid]=round(unit,2)
    return {pid:price for pid,price in results.items()
            if len(encountered_raw_prices[pid])==1}

def apply_archival_quote_references(conn, latest_batch, historical_documents):
    """Pure/database implementation; historical_documents yields (batch, [docs])."""
    ensure_schema_and_migrations(conn)
    latest=datetime.strptime(latest_batch,'%Y%m%d%H%M%S').replace(tzinfo=TW)
    cutoff=latest-timedelta(days=7)
    current={}
    for sid,pid,unit,is_deal in conn.execute("""
        SELECT p.store_uuid,p.product_uuid,p.effective_price,p.seven_day_is_price_deal
        FROM products p JOIN stores s ON p.store_uuid=s.store_uuid
        WHERE p.status='active' AND s.status='active'
          AND p.last_seen=? AND s.last_seen=?
          AND p.is_open=1 AND s.is_open=1 AND p.effective_price>0
    """,(latest.isoformat(),latest.isoformat())):
        current[(sid,pid)]=(round(float(unit),2),bool(is_deal))
    # Every run resets old references, so expired seven-day quotes disappear.
    conn.execute("""UPDATE products SET archive_reference_price=NULL,
       archive_discount_pct=0,archive_discount_amount=0,
       archive_evidence_count=0,archive_is_price_deal=0""")
    repeated=collections.defaultdict(lambda:collections.defaultdict(set))
    batches=0
    stores=0
    for batch,docs in historical_documents:
        try:moment=datetime.strptime(batch,'%Y%m%d%H%M%S').replace(tzinfo=TW)
        except ValueError:continue
        if not cutoff<=moment<latest:continue
        batches+=1
        for doc in docs:
            if not archival_quote_eligible(doc,batch):continue
            sid,_=store_identity(doc)
            stores+=1
            for pid,unit in canonical_menu_quotes(doc,sid).items():
                key=(sid,pid)
                if key in current:
                    repeated[key][unit].add(batch)
    rows=[]
    candidates=[]
    confirmed=0
    for (sid,pid),by_price in repeated.items():
        corroborated=[(price,batches) for price,batches in by_price.items() if len(batches)>=2]
        if not corroborated:continue
        confirmed+=1
        price,batches_for_price=max(corroborated,key=lambda t:t[0])
        current_price,official=current[(sid,pid)]
        comparison=PriceChange.compare(price,current_price)
        if not comparison or not PriceChange.qualifies(comparison,30) or official:
            continue
        evidence=len(batches_for_price)
        rows.append((price,comparison['discount_pct'],comparison['savings'],evidence,1,sid,pid))
        candidates.append({'store':sid,'product':pid,'reference':price,'current':current_price,
                           'pct':comparison['discount_pct'],'batches':evidence})
    conn.executemany("""UPDATE products SET archive_reference_price=?,
        archive_discount_pct=?,archive_discount_amount=?,archive_evidence_count=?,
        archive_is_price_deal=? WHERE store_uuid=? AND product_uuid=?""",rows)
    conn.commit()
    result={'raw_batches':batches,'eligible_stores':stores,
            'products_with_corroborated_quotes':confirmed,
            'archive_quote_candidates':len(candidates),'samples':candidates[:12]}
    print('HF_ARCHIVE_QUOTES_RESTORED='+json.dumps(result,ensure_ascii=False),flush=True)
    return result

def restore_archive_price_references(db_path, config, current_batch):
    """Load SHA-checked seven-day Raw archives in Actions, never rewrite HF Raw."""
    from huggingface_hub import HfApi,hf_hub_download
    api=HfApi(token=os.environ.get('HF_TOKEN'))
    folder=config['hf_folder'].strip('/')
    paths=api.list_repo_files(repo_id=config['hf_repo_id'],repo_type='dataset')
    latest=datetime.strptime(current_batch,'%Y%m%d%H%M%S').replace(tzinfo=TW)
    cutoff=latest-timedelta(days=7)
    manifests=[]
    for path in paths:
        if not (path.startswith(folder+'/snapshots/') and path.endswith('/complete.json')):
            continue
        try:stamp=datetime.strptime(path.split('/')[-2],'%Y%m%d%H%M%S').replace(tzinfo=TW)
        except ValueError:continue
        if cutoff<=stamp<latest:manifests.append(path)
    manifests.sort()
    def historical_documents():
        for path in manifests:
            metadata=json.loads(Path(hf_hub_download(config['hf_repo_id'],path,repo_type='dataset')).read_text(encoding='utf-8'))
            archive=Path(hf_hub_download(config['hf_repo_id'],metadata['archive_path'],repo_type='dataset'))
            if hashlib.sha256(archive.read_bytes()).hexdigest()!=metadata['sha256']:
                raise ValueError('HF archived-price SHA mismatch: '+path)
            docs=[]
            with tarfile.open(archive,'r:gz') as tar:
                for member in tar:
                    if not (member.isfile() and member.name.startswith('Json/') and member.name.endswith('.json')):
                        continue
                    with tar.extractfile(member) as source:docs.append(json.load(source))
            if len(docs)!=metadata['store_count']:
                raise ValueError('HF archived-price item count mismatch: '+path)
            yield metadata['batch_id'],docs
    with sqlite3.connect(db_path) as conn:
        return apply_archival_quote_references(conn,current_batch,historical_documents())
