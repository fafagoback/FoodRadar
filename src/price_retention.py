"""Physical rolling native price-history expiry for FR and FRT."""
from datetime import datetime, timedelta, timezone
import json

RETENTION_DAYS = 60
TW = timezone(timedelta(hours=8))

def retention_cutoff(now=None):
    return ((now or datetime.now(TW)).astimezone(TW)-timedelta(days=RETENTION_DAYS)).isoformat(timespec="seconds")

def _at(value):
    at=datetime.fromisoformat(value)
    if at.tzinfo is None:
        raise ValueError("Price observation timestamp must include timezone")
    return at

def _runs(indices):
    # A batch dictionary can be unsorted after partition rebuilding.
    out=[]
    for new,old in enumerate(indices):
        if out and old==out[-1][0]+out[-1][2]:
            a,b,n=out[-1]
            out[-1]=(a,b,n+1)
        else:
            out.append((old,new,1))
    return out

def _remap(bits,runs):
    result=0
    for old,new,length in runs:
        result|=((bits>>old)&((1<<length)-1))<<new
    return result

def prune_part(part,cutoff):
    old=part.batches
    boundary=_at(cutoff)
    indices=[i for i,(_,at) in enumerate(old) if _at(at)>=boundary]
    if len(indices)==len(old):
        return 0
    runs=_runs(indices)
    keep=[old[i] for i in indices]
    removed=0
    with part.conn:
        for sid in sorted(part.ids):
            original=part.payload_codec.unpack(part._payload(sid,"price_history"))
            pruned=[]
            for ordinal,variants in original:
                remaining=[]
                for fields,mask in variants:
                    bits=int.from_bytes(mask,"little")
                    if bits.bit_length()>len(old):
                        raise ValueError("Unknown compact price batch")
                    retained=_remap(bits,runs)
                    removed+=bits.bit_count()-retained.bit_count()
                    if retained:
                        remaining.append([fields,retained.to_bytes((retained.bit_length()+7)//8,"little")])
                if remaining:
                    pruned.append([ordinal,remaining])
            part._replace("price_history",sid,pruned)
        part.batches=keep
        part.bidx={x:i for i,x in enumerate(keep)}
        part.dirty.add("__batch_keys")
        part.flush()
    part.conn.execute("VACUUM")
    if part.conn.execute("PRAGMA quick_check").fetchone()[0]!="ok":
        raise ValueError("Price retention database integrity failure")
    return removed

def prune_price_history(state,now=None):
    cutoff=retention_cutoff(now)
    removed=sum(prune_part(part,cutoff) for part in state.parts)
    state.meta["price_history_retention_days"]=str(RETENTION_DAYS)
    state.meta["price_history_cutoff"]=cutoff
    state.flush()
    print("FOODRADAR_PRICE_RETENTION="+json.dumps({"days":RETENTION_DAYS,"cutoff":cutoff,"removed_observations":removed}),flush=True)
    return removed
