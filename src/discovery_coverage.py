"""Explicit, auditable coordinate plan. No upstream request until selected."""
import hashlib,json,math,re,os
from pathlib import Path
SOURCE="https://geo.maderaojen.me/datasets/tw-townships/releases/v1/2026-07-04.1/data.geojson"
SOURCE_SHA256="4ab5da8297a75c3e3fd2cb272642693a91f04299a739897dffc8d7ae029d5196"
COUNTIES=set("臺北市 新北市 基隆市 桃園市 新竹市 新竹縣 苗栗縣 臺中市 彰化縣 南投縣 雲林縣 嘉義市 嘉義縣 臺南市 高雄市 屏東縣 宜蘭縣 花蓮縣 臺東縣 澎湖縣 金門縣 連江縣".split())

def clean(s):return str(s).replace("台","臺")

def validate_points(points):
    if not isinstance(points,list) or not points:raise ValueError("Empty discovery plan")
    ids=set()
    for p in points:
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}",p["id"]) or p["id"] in ids:
            raise ValueError("Invalid/duplicate discovery point ID")
        ids.add(p["id"])
        lat,lon=p["latitude"],p["longitude"]
        if not all(type(x) in (int,float) and math.isfinite(x) for x in (lat,lon)) or not (10<=lat<=27 and 114<=lon<=125):
            raise ValueError("Discovery point outside Taiwan boundaries")
        if not p.get("county"):raise ValueError("Missing discovery county")
    return points

def township_points(document,spacing=.04,require_national=True):
    """Interior grid plus one representative point per polygon component.
    Spacing is angular (~4 km), not a claim about delivery-radius coverage.
    """
    from shapely.geometry import shape,Point
    if type(spacing) not in (int,float) or not .01<=spacing<=.1:
        raise ValueError("Unsupported discovery spacing")
    features=document["features"]
    points=[];districts=set();counties=set()
    for feature in features:
        props={str(k).upper():v for k,v in feature["properties"].items()}
        county=clean(props.get("COUNTYNAME") or props.get("COUNTY") or props.get("COUNTY_NAME") or "")
        town=clean(props.get("TOWNNAME") or props.get("TOWN") or props.get("TOWN_NAME") or props.get("NAME") or "")
        code=str(props.get("TOWNCODE") or props.get("TOWNID") or props.get("CODE") or feature.get("id") or "")
        if not county or not town:raise ValueError("Unrecognized township identity: "+repr(sorted(props)))
        identity=(county,town)
        if identity in districts:raise ValueError("Duplicate district")
        districts.add(identity);counties.add(county)
        if not code:code=hashlib.sha256((county+town).encode()).hexdigest()[:12]
        if not re.fullmatch("[A-Za-z0-9_-]+",code):code=hashlib.sha256(code.encode()).hexdigest()[:12]
        geom=shape(feature["geometry"])
        if not geom.is_valid:
            from shapely import make_valid
            geom=make_valid(geom)
        parts=[geom] if geom.geom_type=="Polygon" else list(geom.geoms)
        local={}
        for part in parts:
            if part.geom_type!="Polygon" or part.is_empty:continue
            # Include a representative interior point for isolated islands.
            p=part.representative_point()
            local[(round(p.y,7),round(p.x,7))]=None
            minx,miny,maxx,maxy=part.bounds
            for iy in range(math.ceil(miny/spacing),math.floor(maxy/spacing)+1):
                for ix in range(math.ceil(minx/spacing),math.floor(maxx/spacing)+1):
                    x,y=round(ix*spacing,7),round(iy*spacing,7)
                    if part.covers(Point(x,y)):local[(y,x)]=None
        if not local:raise ValueError("District has no valid interior point")
        for index,(lat,lon) in enumerate(sorted(local)):
            points.append({"id":f"{code}-{index:04d}","latitude":lat,"longitude":lon,
                           "county":county,"town":town})
    if require_national and (len(districts)!=368 or counties!=COUNTIES):
        raise ValueError("Expected 368 districts and 22 counties")
    return validate_points(sorted(points,key=lambda p:p["id"]))

def discovery_scope(config):
    return os.getenv("FR_DISCOVERY_SCOPE") or config.get("discovery_scope","single")

def get_points(config):
    scope=discovery_scope(config)
    if scope=="single":
        return validate_points([{"id":"foodradar","latitude":config["latitude"],
                    "longitude":config["longitude"],"county":"台北市"}])
    if scope=="custom":
        return validate_points(config["discovery_points"])
    if scope!="taiwan":raise ValueError("Unknown discovery scope")
    import requests
    r=requests.get(SOURCE,timeout=60);r.raise_for_status()
    if hashlib.sha256(r.content).hexdigest()!=SOURCE_SHA256:
        raise ValueError("Township boundary hash changed; refusing discovery")
    return township_points(r.json(),config.get("discovery_grid_spacing",.04))

def merge_store_points(previous,item):
    old=previous.setdefault("discovered_points",[])
    found={p["point_id"] for p in old}
    old.extend(p for p in item.get("discovered_points",[]) if p["point_id"] not in found)
    for k,v in item.items():
        if k!="discovered_points" and v is not None and not previous.get(k):previous[k]=v
    # Keep the actual closest observed delivery point first, never invent a
    # location or fetch the same store from every discovered area.
    try:
        lat,lon=float(previous["store_lat"]),float(previous["store_lon"])
        if not all(math.isfinite(x) for x in (lat,lon)):raise ValueError()
        old.sort(key=lambda p:((p["lat"]-lat)**2+(p["lon"]-lon)**2*math.cos(math.radians(lat))**2,p["point_id"]))
    except (KeyError,ValueError,TypeError):old.sort(key=lambda p:p["point_id"])
    return previous
