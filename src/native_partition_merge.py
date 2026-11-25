"""Assemble nested native tasks using original HTTP evidence; never accept partial menus."""
import json
from catalog_complete import _catalog_skus, _catalog_entries

def merge_partition_tree(group,key,docs):
    root=group["sections"][int(key.split(".")[0])]
    parts=group.get("partitions",{})
    def merge(node):
        sec,checks,raw,prefix=docs[node]
        if sec.get("success") is not True:
            raise RuntimeError(str(sec.get("error") or "Unverified native category"))
        direct=sorted((k for k in parts if k.rsplit(".",1)[0]==node),
                      key=lambda k:int(k.rsplit(".",1)[1]))
        expected=parts.get(node,{}).get("sub_path",[])
        def read_pages(records):
            bodies=[]
            for page in records:
                if page.get("path") not in checks or page.get("section_uuid")!=root:
                    raise ValueError("Missing or incorrect original native HTTP response")
                bodies.append(json.loads((raw/page["path"]).read_bytes()))
            return bodies
        def paths(records):
            return [{**page,"path":prefix+page["path"]} for page in records]
        deferred=sec.get("catalog_deferred_children")
        if deferred is not None:
            children=[parts[k]["sub_path"][-1] for k in direct]
            if (not direct or children!=deferred.get("children") or
                    sec.get("catalog_complete") is not False or sec.get("catalog_responses")):
                raise ValueError("Deferred native child roster mismatch")
            preview=deferred.get("preview_responses") or []
            bodies=read_pages(preview)
            if (not bodies or _catalog_entries(bodies[-1])[0]["pagingInfo"]["hasMore"] is not True
                    or any(p.get("subsection_uuid")!=(expected[-1] if expected else None) for p in preview)):
                raise ValueError("Native parent preview is not an unfinished partition")
            parent_skus=_catalog_skus(bodies)
            pages=[]; sku=set(); requests=len(checks); child_count=len(direct)
            for child in direct:
                cp,cs,cr,cc=merge(child)
                pages.extend(cp); sku.update(cs); requests+=cr; child_count+=cc
            missing=parent_skus-sku
            if missing:
                raise RuntimeError(f"Subsections for {root} omitted {len(missing)} previously seen native parent SKUs; refusing partial menu")
            if not pages:
                raise RuntimeError("Native child partition has no verified terminal pages")
            return pages,sku,requests,child_count
        if direct:
            raise ValueError("Unexpected children on terminal native partition")
        reports=sec.get("catalog_section_reports") or []
        if sec.get("catalog_complete") is not True:
            raise RuntimeError(str(sec.get("error") or "Unverified native category"))
        if (len(reports)!=1 or reports[0].get("section_uuid")!=root
                or reports[0].get("terminal_has_more") is not False):
            raise ValueError("Native category terminal pagination unverified")
        records=sec.get("catalog_responses") or []
        bodies=read_pages(records)
        if expected and any(p.get("subsection_uuid")!=expected[-1] for p in records):
            raise ValueError("Wrong native subsection result")
        return paths(records),_catalog_skus(bodies),len(checks),int(reports[0].get("subsections") or 0)
    pages,sku,requests,children=merge(key)
    return pages,{"section_uuid":root,"pages":len(pages),
        "item_occurrences":sum(p["item_count"] for p in pages),
        "unique_skus":len(sku),"subsections":children,"terminal_has_more":False,
        "coverage_evidence":"observed_parent_subset_and_terminal_children" if children else "terminal_pagination",
        "mode":"native_children_before_deep_offset" if children else "section",
        "workload_ms":requests*250}
