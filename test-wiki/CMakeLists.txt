cmake_minimum_required (VERSION 3.1)
set (CMAKE_CXX_STANDARD 17)


project (SearchTestWiki)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")



file(GLOB Sources "*.cpp")
file(GLOB SourcesC "*.c")
file(GLOB HeadersC "*.h")
file(GLOB Headers "*.hpp")

add_executable(${PROJECT_NAME} ${Sources} ${SourcesC} ${HeadersC} ${Headers})



#add_subdirectory("../" "search")
target_link_libraries(${PROJECT_NAME} Search )
INCLUDE_DIRECTORIES( ${SEARCH_INCLUDE_DIRS} )



# ICU
#
# command line add -DCMAKE_PREFIX_PATH=/usr/local/opt/icu4c
# GUI add entry:    CMAKE_PREFIX_PATH     /usr/local/opt/icu4c
#



#### Boost ####
FIND_PACKAGE( Boost 1.60 COMPONENTS system date_time iostreams REQUIRED )   # na windowsih še regex
INCLUDE_DIRECTORIES( ${Boost_INCLUDE_DIR} )
TARGET_LINK_LIBRARIES( ${PROJECT_NAME} ${Boost_LIBRARIES} )