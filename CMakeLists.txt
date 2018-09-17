cmake_minimum_required(VERSION 2.8.12)
project(swipl-odbc)

include("../cmake/PrologPackage.cmake")

find_package(ODBC)
if(ODBC_FOUND)
include(CheckTypeSize)
set(CMAKE_REQUIRED_INCLUDES ${CMAKE_REQUIRED_INCLUDES} ${ODBC_INCLUDE_DIRS})
set(CMAKE_EXTRA_INCLUDE_FILES ${CMAKE_EXTRA_INCLUDE_FILES} sql.h)

AC_CHECK_HEADERS(malloc.h time.h)
AC_CHECK_FUNCS(localtime mktime gmtime timegm)

check_type_size("long" SIZEOF_LONG)
check_type_size("long long" SIZEOF_LONG_LONG)
check_type_size("wchar_t" SIZEOF_WCHAR_T)
check_type_size(SQLWCHAR SIZEOF_SQLWCHAR)
check_type_size(SQLLEN SIZEOF_SQLLEN)
check_type_size(SQLULEN SIZEOF_SQLULEN)
if(NOT SIZEOF_SQLLEN STREQUAL "")
  set(HAVE_SQLLEN 1)
endif()
if(NOT SIZEOF_SQLULEN STREQUAL "")
  set(HAVE_SQLULEN 1)
endif()

configure_file(config.h.cmake config.h)

swipl_plugin(
    odbc4pl
    C_SOURCES odbc.c
    C_LIBS ${ODBC_LIBRARIES}
    PL_LIBS odbc.pl)
target_include_directories(
    plugin_odbc4pl BEFORE PRIVATE
    ${ODBC_INCLUDE_DIR})

# Doesn't work (also not for the original autoconf version)
# test_libs(odbc)

pkg_doc(odbc
	SOURCES odbc.bib)

endif(ODBC_FOUND)
