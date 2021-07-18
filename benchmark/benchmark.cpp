#include "sqlite/ext/lsm1/lsm.h"

#include <chrono>
#include <stdexcept>
#include <array>
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
    start = clock::now();
    CHECK_LSM_RC(lsm_csr_open(lsmDb, &lsmCursor));
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
    CHECK_LSM_RC(lsm_csr_close(lsmCursor));

    start = clock::now();
    for (const auto& key : keys) {
        CHECK_LSM_RC(lsm_csr_open(lsmDb, &lsmCursor));
        CHECK_LSM_RC(lsm_csr_seek(lsmCursor, key.data(), key.size(), LSM_SEEK_EQ));
        assert(lsm_csr_valid(lsmCursor) == 1);
        const char* valuePtr;
        int valueSize;
        CHECK_LSM_RC(lsm_csr_value(lsmCursor, reinterpret_cast<const void**>(&valuePtr), &valueSize));
        CHECK_LSM_RC(lsm_csr_close(lsmCursor));
    }
    std::cout << "[lsm1] Read all " << keys.size() << " values sequentially: " << std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - start).count() << " ms\n";

    CHECK_LSM_RC(lsm_close(lsmDb));

    std::FILE* file = fopen64("shops-7f00b33a8134aa21f40d1295bc80b5ee_177.rofldb", "rb");
    if (!file) {
        throw std::runtime_error(std::strerror(errno));
    }
    struct stat64 fileStat {};
    fstat64(fileno(file), &fileStat);

    auto* data = static_cast<std::byte*>(mmap(nullptr, fileStat.st_size, PROT_READ, MAP_PRIVATE, fileno(file), 0));
    if (data == MAP_FAILED) {
        throw std::runtime_error(std::strerror(errno));
    }
    fclose(file);

    RoflDb::DbReader dbReader(data, fileStat.st_size);
    start = clock::now();
    for (const auto& key : keys) {
        if (!dbReader.get(key)) {
            std::cout << "ERROR";
            return 2;
        }
    }
    std::cout << "[RoflDb::DbReader] Read all " << keys.size() << " values sequentially: " << std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - start).count() << " ms\n";

    return 0;
}
