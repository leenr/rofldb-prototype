#include <cstddef>
#include <cstdio>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cstring>
#include <iostream>
#include <map>
#include <chrono>

#include "include/library.h"

#define MAP_HUGE_8MB    (23 << MAP_HUGE_SHIFT)

int main(int argc, char* argv[]) {
    using clock = std::chrono::steady_clock;
    decltype(clock::now()) start, end;

    if (argc <= 1) {
        std::map<std::string, std::string> map;

        start = clock::now();
        for (unsigned long long i = 0; i < 30000000; i++) {
            map["key" + std::to_string(i)] = "value" + std::to_string(i);
        }
        std::cout << "Write " << 30000000 << ": " << std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - start).count() << " ms\n";

        start = clock::now();
        for (unsigned long long i = 0; i < 1000000; i++) {
            map["key" + std::to_string(i)];
        }
        std::cout << "Read " << 1000000 << ": " << std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - start).count() << " ms\n";
        return 1;
    }

    std::FILE* file = fopen64(argv[1], "rb");
    if (errno) {
        throw std::runtime_error(std::strerror(errno));
    }
    struct stat64 fileStat {};
    fstat64(fileno(file), &fileStat);

    auto* data = static_cast<std::byte*>(mmap(nullptr, fileStat.st_size, PROT_READ, MAP_PRIVATE | MAP_HUGE_8MB, fileno(file), 0));
    if (data == MAP_FAILED) {
        throw std::runtime_error(std::strerror(errno));
    }
    fclose(file);

    RoflDb::DbReader dbReader(data, fileStat.st_size);
    start = clock::now();
    for (long long i = 0; i < 1000000; i++) {
        auto key = "key" + std::to_string(i);
        if (auto res = dbReader.get(key)) {
//            std::cerr << "Key '" << key << "' found: " << std::string(reinterpret_cast<const char*>(res->get()), res->size()) << "\n";
        } else {
//            std::cerr << "Not found key '" << key << "'!\n";
        }
    }
    std::cout << "Read " << 1000000 << ": " << std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - start).count() << " ms\n";
    std::cin.get();
    munmap(data, fileStat.st_size);
    std::cin.get();
    return 0;
}
