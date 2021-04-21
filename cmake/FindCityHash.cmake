# CITYHASH_INCLUDE_DIR, CITYHASH_LIBRARIES

# CITYHASH_INCLUDE_DIRS	- where to find city.h, etc.
# CITYHASH_LIBRARIES	- List of libraries when using sqlite.
# CITYHASH_FOUND	- True if sqlite found.

# Look for the header file.
FIND_PATH(CITYHASH_INCLUDE_DIR NAMES city.h)

# Look for the library.
FIND_LIBRARY(CITYHASH_LIBRARY NAMES libcityhash.a)

# Handle the QUIETLY and REQUIRED arguments and set CITYHASH_FOUND to TRUE if all listed variables are TRUE.
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(CITYHASH DEFAULT_MSG CITYHASH_LIBRARY CITYHASH_INCLUDE_DIR)

# Copy the results to the output variables.
IF(CITYHASH_FOUND)
	SET(CITYHASH_LIBRARIES ${CITYHASH_LIBRARY})
	SET(CITYHASH_INCLUDE_DIRS ${CITYHASH_INCLUDE_DIR})
ELSE(CITYHASH_FOUND)
	SET(CITYHASH_LIBRARIES)
	SET(CITYHASH_INCLUDE_DIRS)
ENDIF(CITYHASH_FOUND)

MARK_AS_ADVANCED(CITYHASH_INCLUDE_DIRS CITYHASH_LIBRARIES)