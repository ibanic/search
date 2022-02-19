cmake_minimum_required(VERSION 3.1)
set(CMAKE_CXX_STANDARD 17)

project(Search)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# sources
set(Sources
  ./src/CompressSize.cpp
  ./src/KeyValueFile.cpp
  ./src/KeyValueFileList.cpp
  ./src/LoadExcerpt.cpp
  ./src/Tokenize.cpp
)

# Headers
set(Headers
  ./include/search/Comparators.hpp
  ./include/search/CompressSize.hpp
  ./include/search/Db.hpp
  ./include/search/DocSimple.hpp
  ./include/search/FileStore.hpp
  ./include/search/FindMany.hpp
  ./include/search/KeyValueFile.hpp
  ./include/search/KeyValueFileList.hpp
  ./include/search/KeyValueMemory.hpp
  ./include/search/LoadExcerpt.hpp
  ./include/search/MemoryStore.hpp
  ./include/search/SearchSettings.hpp
  ./include/search/Sort.hpp
  ./include/search/TokenInfo.hpp
  ./include/search/Tokenize.hpp
  ./include/search/Types.hpp
)

# Library
add_library(${PROJECT_NAME} STATIC ${Sources} ${Headers})
set_property(TARGET ${PROJECT_NAME} PROPERTY POSITION_INDEPENDENT_CODE ON)

# Threads
find_package(Threads)
target_link_libraries(${PROJECT_NAME} ${CMAKE_THREAD_LIBS_INIT})

# Filesystem
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_link_libraries(${PROJECT_NAME} stdc++fs)
endif()

# Boost
find_package(Boost 1.60 COMPONENTS system iostreams REQUIRED)
include_directories(${Boost_INCLUDE_DIR})
target_link_libraries(${PROJECT_NAME} ${Boost_LIBRARIES})

# hopscotch map
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/tessil-hopscotch-map/include")

# ICU
# command line add -DCMAKE_PREFIX_PATH=/usr/local/opt/icu4c
# GUI add entry:    CMAKE_PREFIX_PATH     /usr/local/opt/icu4c
find_package(ICU 60.0 COMPONENTS uc i18n REQUIRED)
target_link_libraries(${PROJECT_NAME} ICU::uc ICU::i18n)

# CityHash
find_package(CityHash REQUIRED)
include_directories(${CITYHASH_INCLUDE_DIRS})
target_link_libraries(${PROJECT_NAME} ${CITYHASH_LIBRARIES})

# Include
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
set(SEARCH_INCLUDE_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/include;${CMAKE_CURRENT_SOURCE_DIR}/tessil-hopscotch-map/include")
set(SEARCH_INCLUDE_DIRS ${SEARCH_INCLUDE_DIRS} PARENT_SCOPE)

# Test
if(SEARCH_BUILD_TEST)
  add_subdirectory(test)
  # default startup project for Visual Studio
  if(MSVC)
    set_property(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY VS_STARTUP_PROJECT SearchTest)
    set_property(TARGET testbed PROPERTY VS_DEBUGGER_WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/SearchTest")
  endif()
endif()
