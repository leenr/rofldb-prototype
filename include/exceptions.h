#pragma once

#include <stdexcept>

#ifndef ROFLDB_SECURITY
#define ROFLDB_SECURITY true
#endif

namespace RoflDb::Exceptions {

class data_corrupted_error : public std::runtime_error {
public:
    explicit data_corrupted_error(auto reason) : std::runtime_error(reason) {};
};

class magic_error : public std::runtime_error {
public:
    explicit magic_error(auto reason) : std::runtime_error(reason) {};
};

}
