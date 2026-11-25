"""Optional trained dictionary, local to one compact DB, no external dependency.
The physical dictionary is charged to the SQLite file and never contains
live mutable branch state references. Legacy standard zstd frames remain readable.
"""
import msgpack
import zstandard as zstd
KEY="__payload_dictionary_v1"
MAGIC=b"FRD1"

class PayloadCodec:
    def __init__(self,dictionary=None):
        self.dictionary=dictionary or b""
        if type(self.dictionary) is not bytes:raise ValueError("Invalid payload dictionary")
        self.plain=zstd.ZstdDecompressor()
        self.trained=zstd.ZstdCompressionDict(self.dictionary) if self.dictionary else None
        self.decoder=zstd.ZstdDecompressor(dict_data=self.trained) if self.trained else None
        self.encoder=zstd.ZstdCompressor(level=10,dict_data=self.trained,write_content_size=True) if self.trained else None
        self.standard=zstd.ZstdCompressor(level=10,write_content_size=True)
    def raw(self,blob):
        if blob.startswith(MAGIC):
            if self.decoder is None:raise ValueError("Missing trained payload dictionary")
            return self.decoder.decompress(blob[len(MAGIC):])
        return self.plain.decompress(blob)
    def unpack(self,blob):
        return msgpack.unpackb(self.raw(blob),raw=False,strict_map_key=False)
    def pack(self,value):
        raw=msgpack.packb(value,use_bin_type=True)
        standard=self.standard.compress(raw)
        if self.encoder is None:return standard
        shared=MAGIC+self.encoder.compress(raw)
        return min(standard,shared,key=len)
    def encode_candidates(self,raw,levels=(10,12,16)):
        candidates=[]
        for level in levels:
            candidates.append(zstd.ZstdCompressor(level=level,write_content_size=True).compress(raw))
            if self.trained:
                candidates.append(MAGIC+zstd.ZstdCompressor(level=level,dict_data=self.trained,write_content_size=True).compress(raw))
        return min(candidates,key=len)
