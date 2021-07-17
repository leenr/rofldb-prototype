#include <cstddef>
#include <cstdio>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cstring>
#include <iostream>

#include "include/library.h"

#define MAP_HUGE_2MB    (21 << MAP_HUGE_SHIFT)

int main(int argc, char* argv[]) {
    if (argc <= 1) {
        return 1;
    }

    std::FILE* file = fopen64(argv[1], "rb");
    if (errno) {
        throw std::runtime_error(std::strerror(errno));
    }
    struct stat64 fileStat {};
    fstat64(fileno(file), &fileStat);

    auto* data = static_cast<std::byte*>(mmap(nullptr, fileStat.st_size, PROT_READ, MAP_PRIVATE | MAP_HUGE_2MB, fileno(file), 0));
    if (data == MAP_FAILED) {
        throw std::runtime_error(std::strerror(errno));
    }
    fclose(file);

    RoflDb::DbReader dbReader(data, fileStat.st_size);
    for (long long i = 0; i < 1000000; i++) {
        auto key = "key" + std::to_string(i);
        if (auto res = dbReader.get(key)) {
            std::cerr << "Key '" << key << "' found: " << std::string(reinterpret_cast<const char*>(res->get()), res->size()) << "\n";
        } else {
            std::cerr << "Not found key '" << key << "'!\n";
        }
    }
    return 0;
}
