"""Local real Git contention with 30,000 synthetic stores; never calls upstream."""
import concurrent.futures,json,subprocess,sys,tempfile,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from crawl_queue import SharedGitQueue,initial_state

def git(path,*args):
    return subprocess.run(["git","-C",str(path),*args],check=True,capture_output=True,text=True)

def main():
    with tempfile.TemporaryDirectory() as t:
        root=Path(t)
        remote=root/"remote.git";remote.mkdir();git(remote,"init","--bare")
        clients=[]
        for i in range(15):
            p=root/f"worker-{i}";p.mkdir();git(p,"init");git(p,"remote","add","origin",str(remote))
            clients.append(SharedGitQueue(p,"stress",lease_seconds=600))
        ids=[f"store-{i:06d}" for i in range(30000)]
        start=time.perf_counter()
        clients[0].initialize(initial_state("stress","epoch","sha",ids))
        init_s=time.perf_counter()-start
        start=time.perf_counter()
        def worker(i):
            q=clients[i];out=[]
            for _ in range(2):
                claims=q.claim(f"owner-{i}",i,12)
                assert len(claims)==12
                for sid,lease in claims:
                    assert q.finish(sid,lease["token"],int(sid[-6:])+1)
                    out.append(sid)
            return out
        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
            completed=sum(executor.map(worker,range(15)),[])
        work_s=time.perf_counter()-start
        start=time.perf_counter();state=clients[0].read();read_s=time.perf_counter()-start
        assert len(completed)==360==len(set(completed))
        assert set(state["completed"])==set(completed)
        assert len(state["pending"])==30000-360 and not state["leased"]
        print("FR_LARGE_QUEUE_STRESS="+json.dumps({"roster":len(ids),"logical_workers":15,
            "real_git_completed":len(completed),"duplicate":0,"pending":len(state["pending"]),
            "initialize_seconds":round(init_s,3),"work_seconds":round(work_s,3),
            "read_seconds":round(read_s,3),"upstream_requests":0}),flush=True)
if __name__=="__main__":main()
