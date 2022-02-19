cmake_minimum_required(VERSION 3.1)
set(CMAKE_CXX_STANDARD 17)

project(SearchTest)
enable_testing()

set(Headers_${PROJECT_NAME}
  Mocks.hpp
)

set(Sources_${PROJECT_NAME}
  test1.cpp
)

add_executable(${PROJECT_NAME}
  ${Headers_${PROJECT_NAME}}
  ${Sources_${PROJECT_NAME}}
)

find_package(GTest REQUIRED)
target_link_libraries(${PROJECT_NAME} Search GTest::gtest GTest::gmock GTest::gtest_main)
gtest_discover_tests(${PROJECT_NAME})
