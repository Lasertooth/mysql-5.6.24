# TODO: Copyrights
#
# Optional environment variables that may be used, if library
# files are located in non-standard path:
#

SET(COCKROACHDB_SE_PLUGIN_STATIC  "cockroachdb")
SET(COCKROACHDB_SE_PLUGIN_MANDATORY TRUE)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --std=c++11")

SET(COCKROACHDB_SOURCES
      ha_cockroach.cc ha_cockroach.h
      #rdb_datadic.cc rdb_datadic.h
      rowlock.cc rowlock.h
      cocdb.grpc.pb.cc cocdb.grpc.pb.h
      cocdb.pb.cc cocdb.pb.h
      datadic.cc datadic.h
      cockroachdb.cc cockroachdb.h
      cache_trx.cc cache_trx.h
      row_table.cc row_table.h
      applyiter.cc applyiter.h)
      #rdb_applyiter.cc rdb_applyiter.h)

MYSQL_ADD_PLUGIN(cockroachdb_se ${COCKROACHDB_SOURCES} STORAGE_ENGINE STATIC_ONLY MANDATORY LINK_LIBRARIES ${LINK_DIR} snappy bz2 grpcxx_unsecure grpc gpr protobuf pthread dl lz4)
