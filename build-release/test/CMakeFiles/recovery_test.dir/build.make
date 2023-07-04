# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.16

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /15-445/bustub-master

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /15-445/bustub-master/build-release

# Include any dependencies generated for this target.
include test/CMakeFiles/recovery_test.dir/depend.make

# Include the progress variables for this target.
include test/CMakeFiles/recovery_test.dir/progress.make

# Include the compile flags for this target's objects.
include test/CMakeFiles/recovery_test.dir/flags.make

test/CMakeFiles/recovery_test.dir/recovery/recovery_test.cpp.o: test/CMakeFiles/recovery_test.dir/flags.make
test/CMakeFiles/recovery_test.dir/recovery/recovery_test.cpp.o: ../test/recovery/recovery_test.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/15-445/bustub-master/build-release/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/CMakeFiles/recovery_test.dir/recovery/recovery_test.cpp.o"
	cd /15-445/bustub-master/build-release/test && /usr/bin/clang++-12  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/recovery_test.dir/recovery/recovery_test.cpp.o -c /15-445/bustub-master/test/recovery/recovery_test.cpp

test/CMakeFiles/recovery_test.dir/recovery/recovery_test.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/recovery_test.dir/recovery/recovery_test.cpp.i"
	cd /15-445/bustub-master/build-release/test && /usr/bin/clang++-12 $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /15-445/bustub-master/test/recovery/recovery_test.cpp > CMakeFiles/recovery_test.dir/recovery/recovery_test.cpp.i

test/CMakeFiles/recovery_test.dir/recovery/recovery_test.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/recovery_test.dir/recovery/recovery_test.cpp.s"
	cd /15-445/bustub-master/build-release/test && /usr/bin/clang++-12 $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /15-445/bustub-master/test/recovery/recovery_test.cpp -o CMakeFiles/recovery_test.dir/recovery/recovery_test.cpp.s

# Object files for target recovery_test
recovery_test_OBJECTS = \
"CMakeFiles/recovery_test.dir/recovery/recovery_test.cpp.o"

# External object files for target recovery_test
recovery_test_EXTERNAL_OBJECTS =

test/recovery_test: test/CMakeFiles/recovery_test.dir/recovery/recovery_test.cpp.o
test/recovery_test: test/CMakeFiles/recovery_test.dir/build.make
test/recovery_test: lib/libbustub.a
test/recovery_test: lib/libgtest.a
test/recovery_test: lib/libgmock_main.a
test/recovery_test: lib/libbustub_murmur3.a
test/recovery_test: lib/libduckdb_pg_query.a
test/recovery_test: lib/libfmt.a
test/recovery_test: lib/libfort.a
test/recovery_test: lib/libgmock.a
test/recovery_test: lib/libgtest.a
test/recovery_test: test/CMakeFiles/recovery_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/15-445/bustub-master/build-release/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable recovery_test"
	cd /15-445/bustub-master/build-release/test && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/recovery_test.dir/link.txt --verbose=$(VERBOSE)
	cd /15-445/bustub-master/build-release/test && /usr/bin/cmake -D TEST_TARGET=recovery_test -D TEST_EXECUTABLE=/15-445/bustub-master/build-release/test/recovery_test -D TEST_EXECUTOR= -D TEST_WORKING_DIR=/15-445/bustub-master/build-release/test -D "TEST_EXTRA_ARGS=--gtest_color=auto;--gtest_output=xml:/15-445/bustub-master/build-release/test/recovery_test.xml;--gtest_catch_exceptions=0" -D "TEST_PROPERTIES=TIMEOUT;120" -D TEST_PREFIX= -D TEST_SUFFIX= -D NO_PRETTY_TYPES=FALSE -D NO_PRETTY_VALUES=FALSE -D TEST_LIST=recovery_test_TESTS -D CTEST_FILE=/15-445/bustub-master/build-release/test/recovery_test[1]_tests.cmake -D TEST_DISCOVERY_TIMEOUT=120 -P /usr/share/cmake-3.16/Modules/GoogleTestAddTests.cmake

# Rule to build all files generated by this target.
test/CMakeFiles/recovery_test.dir/build: test/recovery_test

.PHONY : test/CMakeFiles/recovery_test.dir/build

test/CMakeFiles/recovery_test.dir/clean:
	cd /15-445/bustub-master/build-release/test && $(CMAKE_COMMAND) -P CMakeFiles/recovery_test.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/recovery_test.dir/clean

test/CMakeFiles/recovery_test.dir/depend:
	cd /15-445/bustub-master/build-release && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /15-445/bustub-master /15-445/bustub-master/test /15-445/bustub-master/build-release /15-445/bustub-master/build-release/test /15-445/bustub-master/build-release/test/CMakeFiles/recovery_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/recovery_test.dir/depend

