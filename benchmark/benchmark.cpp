#include "sqlite/ext/lsm1/lsm.h"
#include "lmdb/libraries/liblmdb/lmdb.h"

#include <chrono>
#include <stdexcept>
#include <vector>
#include <iostream>
#include <cassert>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <library.h>

#define CHECK_LSM_RC(stmt) if (auto rc = (stmt)) { \
                               throw std::runtime_error(std::string(#stmt) + " failed (" + std::string(__FILE__) + ":" + std::to_string(__LINE__) + "): error #" + std::to_string(rc)); \
                           }
#define CHECK_LMDB_RC(stmt) if (auto rc = (stmt)) { \
                                throw std::runtime_error(std::string(#stmt) + " failed (" + std::string(__FILE__) + ":" + std::to_string(__LINE__) + "): error #" + mdb_strerror(rc)); \
                            }

int main() {
    using clock = std::chrono::steady_clock;
    decltype(clock::now()) start, end;

    lsm_db* lsmDb;
    lsm_env* lsmEnv = lsm_default_env();
    lsm_cursor* lsmCursor;

    int zero = 0;
    int one = 1;

    CHECK_LSM_RC(lsm_new(lsmEnv, &lsmDb));
    CHECK_LSM_RC(lsm_config(lsmDb, LSM_CONFIG_READONLY, &one));
    CHECK_LSM_RC(lsm_config(lsmDb, LSM_CONFIG_MMAP, &zero));
    CHECK_LSM_RC(lsm_open(lsmDb, "shops-7f00b33a8134aa21f40d1295bc80b5ee_177.lsm"));

    std::vector<std::vector<std::byte>> keys;
    CHECK_LSM_RC(lsm_csr_open(lsmDb, &lsmCursor));
    start = clock::now();
    CHECK_LSM_RC(lsm_csr_first(lsmCursor));
    while (lsm_csr_valid(lsmCursor)) {
        const std::byte* key;
        int keySize;
        CHECK_LSM_RC(lsm_csr_key(lsmCursor, reinterpret_cast<const void**>(&key), &keySize));
        auto& keyPair = keys.emplace_back(keySize);
        std::memcpy(keyPair.data(), key, keySize);
        CHECK_LSM_RC(lsm_csr_next(lsmCursor));
    }
    std::cout << "[lsm1] Get all " << keys.size() << " keys sequentially: " << std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - start).count() << " ms\n";
//    CHECK_LSM_RC(lsm_csr_close(lsmCursor));

    start = clock::now();
    for (const auto& key : keys) {
//        CHECK_LSM_RC(lsm_csr_open(lsmDb, &lsmCursor));
        CHECK_LSM_RC(lsm_csr_seek(lsmCursor, key.data(), key.size(), LSM_SEEK_EQ));
        assert(lsm_csr_valid(lsmCursor) == 1);
        const char* valuePtr;
        int valueSize;
        CHECK_LSM_RC(lsm_csr_value(lsmCursor, reinterpret_cast<const void**>(&valuePtr), &valueSize));
//        CHECK_LSM_RC(lsm_csr_close(lsmCursor));
    }
    std::cout << "[lsm1] Read all " << keys.size() << " values sequentially: " << std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - start).count() << " ms\n";

    CHECK_LSM_RC(lsm_csr_close(lsmCursor));
    CHECK_LSM_RC(lsm_close(lsmDb));

    std::FILE* file = fopen64("shops-7f00b33a8134aa21f40d1295bc80b5ee_177.rofldb", "rb");
    if (!file) {
        throw std::runtime_error(std::strerror(errno));
    }
    struct stat64 fileStat {};
    fstat64(fileno(file), &fileStat);

    auto* roflData = static_cast<std::byte*>(mmap(nullptr, fileStat.st_size, PROT_READ, MAP_PRIVATE, fileno(file), 0));
    if (roflData == MAP_FAILED) {
        throw std::runtime_error(std::strerror(errno));
    }
    fclose(file);

    RoflDb::DbReader dbReader(roflData, fileStat.st_size);
    start = clock::now();
    for (const auto& key : keys) {
        if (!dbReader.get(key)) {
            std::cout << "ERROR";
            return 2;
        }
    }
    std::cout << "[RoflDb::DbReader] Read all " << keys.size() << " values sequentially: " << std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - start).count() << " ms\n";

    munmap(roflData, fileStat.st_size);

    MDB_env *lmdbEnv;
    MDB_dbi lmdDbi;
    MDB_val lmdbKey, lmdbData;
    MDB_txn *lmdbTxn;
    MDB_cursor *lmdbCursor;

    CHECK_LMDB_RC(mdb_env_create(&lmdbEnv));
    CHECK_LMDB_RC(mdb_env_open(lmdbEnv, "shops-7f00b33a8134aa21f40d1295bc80b5ee_177.lmdb", MDB_RDONLY, 0444));
    CHECK_LMDB_RC(mdb_txn_begin(lmdbEnv, nullptr, MDB_RDONLY, &lmdbTxn));
    CHECK_LMDB_RC(mdb_dbi_open(lmdbTxn, nullptr, 0, &lmdDbi));
    CHECK_LMDB_RC(mdb_cursor_open(lmdbTxn, lmdDbi, &lmdbCursor));

    start = clock::now();
    for (auto& key : keys) {
        lmdbKey.mv_data = key.data();
        lmdbKey.mv_size = key.size();
        CHECK_LMDB_RC(mdb_get(lmdbTxn, lmdDbi, &lmdbKey, &lmdbData));
    }
    std::cout << "[lmdb] Read all " << keys.size() << " values sequentially: " << std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - start).count() << " ms\n";

    mdb_cursor_close(lmdbCursor);
    mdb_txn_abort(lmdbTxn);
    mdb_dbi_close(lmdbEnv, lmdDbi);
    mdb_env_close(lmdbEnv);

    return 0;
}
