"""Historical HF archive test: old MD5 raw names and new SHA256 raw names."""
import io
import json
import sys
import tarfile
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from archive_pairing import archived_documents_with_native

BATCH="20261029145834"
SID="ffffffff-ffff-4fff-8fff-ffffffffffff"
MD5="a" * 32
SHA="b" * 64
SKU="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def archive(old, remove_raw=False, wrong_uuid=False):
    doc={"store_uuid":SID,"store_id":MD5,"@id":"https://www.ubereats.com/tw/store/a/b",
         "hasMenu":{"hasMenuSection":[{"name":"熱銷商品",
           "hasMenuItem":[{"identifier":SKU,"name":"A","offers":{"price":"60"}}]}]}}
    item={"uuid":SKU,"title":"A","price":6000,"catalogItemAnalyticsData":{"promoType":"BOGO"},
          "itemPromotion":{"type":"buyXGetYItemPromotion",
            "buyXGetYItemPromotion":{"buyQuantity":1,"getQuantity":1}}}
    raw={"data":{"uuid":("00000000-0000-4000-8000-000000000000" if wrong_uuid else SID),
          "catalogSectionsMap":{"a":[{"payload":{"standardItemsPayload":{
              "catalogItems":[item]}}}]}}}
    packed=io.BytesIO()
    with tarfile.open(fileobj=packed,mode="w:gz") as tar:
        docs={"Json/"+BATCH+"_"+SHA+".json":doc}
        if not remove_raw:
            prefix=BATCH+"_"+(MD5+"_restaurant" if old else SHA)
            docs["ApiRaw/"+prefix+".json"]=raw
        for name,obj in docs.items():
            payload=json.dumps(obj).encode()
            info=tarfile.TarInfo(name)
            info.size=len(payload)
            tar.addfile(info,io.BytesIO(payload))
    packed.seek(0)
    return packed


class ArchivePairing(unittest.TestCase):
    def exercise(self,old):
        with tarfile.open(fileobj=archive(old),mode="r:gz") as tar:
            members={m.name:m for m in tar.getmembers()}
            docs,stats=archived_documents_with_native(
                tar,members,BATCH,{"api_raw_response_count":1})
        self.assertEqual(stats["native_matched"],1)
        q=docs[0]["hasMenu"]["hasMenuSection"][0]["hasMenuItem"][0]["verified_price"]
        self.assertEqual(q["kind"],"SAME_SKU_BXGY")
        self.assertEqual(q["received_quantity"],2)

    def test_legacy_url_md5_raw_file(self): self.exercise(True)
    def test_new_canonical_sha256_raw_file(self): self.exercise(False)

    def test_missing_native_archive_cannot_claim_matches(self):
        with tarfile.open(fileobj=archive(True,remove_raw=True),mode="r:gz") as tar:
            members={m.name:m for m in tar.getmembers()}
            with self.assertRaisesRegex(ValueError,"manifest mismatch"):
                archived_documents_with_native(tar,members,BATCH,
                                               {"api_raw_response_count":1})

    def test_raw_store_uuid_mismatch_is_rejected(self):
        with tarfile.open(fileobj=archive(True,wrong_uuid=True),mode="r:gz") as tar:
            members={m.name:m for m in tar.getmembers()}
            with self.assertRaisesRegex(ValueError,"UUID disagreement"):
                archived_documents_with_native(tar,members,BATCH,
                                               {"api_raw_response_count":1})


if __name__=="__main__":
    unittest.main()
