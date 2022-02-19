# CITYHASH_INCLUDE_DIR, CITYHASH_LIBRARIES

# CITYHASH_INCLUDE_DIRS	- where to find city.h, etc.
# CITYHASH_LIBRARIES	- List of libraries when using sqlite.
# CITYHASH_FOUND	- True if sqlite found.

# Look for the header file.
find_path(CITYHASH_INCLUDE_DIR NAMES city.h)

# Look for the library.
find_library(CITYHASH_LIBRARY NAMES libcityhash.a)

# Handle the QUIETLY and REQUIRED arguments and set CITYHASH_FOUND to TRUE if all listed variables are TRUE.
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(CITYHASH DEFAULT_MSG CITYHASH_LIBRARY CITYHASH_INCLUDE_DIR)

# Copy the results to the output variables.
if(CITYHASH_FOUND)
  set(CITYHASH_LIBRARIES ${CITYHASH_LIBRARY})
  set(CITYHASH_INCLUDE_DIRS ${CITYHASH_INCLUDE_DIR})
else(CITYHASH_FOUND)
  set(CITYHASH_LIBRARIES)
  set(CITYHASH_INCLUDE_DIRS)
endif(CITYHASH_FOUND)

mark_as_advanced(CITYHASH_INCLUDE_DIRS CITYHASH_LIBRARIES)
