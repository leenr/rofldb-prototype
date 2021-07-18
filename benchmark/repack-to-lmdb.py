import lmdb
import lsm
import sys


def repack(src_filename: str, target_filename: str):
    with lmdb.open(target_filename, map_size=2 * 1024 ** 3) as lmdb_db:
        with lmdb_db.begin(write=True) as lmdb_txn:
            with lsm.LSM(src_filename, readonly=True) as lsm_db:
                for key, value in lsm_db.items():
                    lmdb_txn.put(key, value)


repack(*sys.argv[1:])
