#include <search/Comparators.hpp>
#include <search/Db.hpp>
#include <search/FileStore.hpp>
#include <search/FindMany.hpp>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

using testing::Return;
using testing::ReturnRef;
using testing::_;
using testing::DoAll;
using testing::Invoke;

using namespace Search;
