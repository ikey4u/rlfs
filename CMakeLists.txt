cmake_minimum_required(VERSION 3.11)
project(rlfs VERSION 1.0)

# Predefined variables
# set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_INSTALL_PREFIX $ENV{HOME}/.local/rlfs)
set(OPENSSL_ROOT_DIR $ENV{HOME}/.local/openssl)

# Find openssl
find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})

# Find fuse2
find_package(PkgConfig)
pkg_check_modules(FUSE2 REQUIRED fuse>=2.9)
# notice that ${FUSE2_CFLAGS} will not work here, so I fix it by hand
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_FILE_OFFSET_BITS=64")
include_directories(${PROJECT_SOURCE_DIR}/src/include ${PROJECT_SOURCE_DIR}/bbfs ${FUSE2_INCLUDE_DIRS})
link_directories(${FUSE2_LIBRARY_DIRS})


# Compile the client
add_executable(rlfsc ${PROJECT_SOURCE_DIR}/src/rlfsc.c ${PROJECT_SOURCE_DIR}/bbfs/log.c)
target_link_libraries(rlfsc -lm ${FUSE2_LDFLAGS} ${OPENSSL_LIBRARIES} )

# Compile the server
file(GLOB RLFS_DAEMON ${PROJECT_SOURCE_DIR}/src/rlfsd.c)
add_executable(rlfsd ${RLFS_DAEMON})
target_link_libraries(rlfsd -lm -lpthread ${OPENSSL_LIBRARIES})

# Install to ${CMAKE_PREFIX_PATH}/bin directory
install(TARGETS rlfsd rlfsc DESTINATION bin)

# For debugging
#message(STATUS \n LIB=${FUSE2_LIBRARY_DIRS} \n INC=${FUSE2_INCLUDE_DIRS} \n LDFLAGS=${FUSE2_LDFLAGS} \n CFLAGS=${FUSE2_CFLAGS})
