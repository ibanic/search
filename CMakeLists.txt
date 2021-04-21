cmake_minimum_required (VERSION 3.1)
set (CMAKE_CXX_STANDARD 17)

project (Search)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

file(GLOB Sources "*.cpp")
file(GLOB HeadersC "*.h")
file(GLOB Headers "*.hpp")
add_library(${PROJECT_NAME} STATIC ${Sources} ${HeadersC} ${Headers})



set_property(TARGET ${PROJECT_NAME} PROPERTY POSITION_INDEPENDENT_CODE ON)
option(SEARCH_BUILD_TEST "Build the Search test program" ON)



#### Boost ####
FIND_PACKAGE( Boost 1.60 COMPONENTS system iostreams REQUIRED )
INCLUDE_DIRECTORIES( ${Boost_INCLUDE_DIR} )
TARGET_LINK_LIBRARIES( ${PROJECT_NAME} ${Boost_LIBRARIES} )


#### hopscotch map ####
INCLUDE_DIRECTORIES("${CMAKE_CURRENT_SOURCE_DIR}/tessil-hopscotch-map/include")



# ICU
#
# command line add -DCMAKE_PREFIX_PATH=/usr/local/opt/icu4c
# GUI add entry:    CMAKE_PREFIX_PATH     /usr/local/opt/icu4c
#
# Components Reference:
# https://cmake.org/cmake/help/latest/module/FindICU.html
# ICU components = data, i18n, io, le, lx, test, tu and uc.
find_package(ICU 60.0 COMPONENTS uc i18n REQUIRED)
# Link using the imported target names (ICU::uc, ICU::i18n, etc.)
target_link_libraries(${PROJECT_NAME} ICU::uc ICU::i18n)



#### CityHash ####
FIND_PACKAGE( CityHash REQUIRED )
INCLUDE_DIRECTORIES( ${CITYHASH_INCLUDE_DIRS} )
TARGET_LINK_LIBRARIES( ${PROJECT_NAME} ${CITYHASH_LIBRARIES} )



#### Threads ####
FIND_PACKAGE(Threads)
TARGET_LINK_LIBRARIES( ${PROJECT_NAME} ${CMAKE_THREAD_LIBS_INIT} )


#### Filesystem ####
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
	TARGET_LINK_LIBRARIES( ${PROJECT_NAME} stdc++fs )
endif()


#### Test ####
if (SEARCH_BUILD_TEST)
	add_subdirectory(test)

	# default startup project for Visual Studio
	if (MSVC)
		set_property(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY VS_STARTUP_PROJECT SearchTest)
		set_property(TARGET testbed PROPERTY VS_DEBUGGER_WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/SearchTest")
	endif()
endif()