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

# Utility rule file for p3.04-index-scan_test.

# Include the progress variables for this target.
include test/CMakeFiles/p3.04-index-scan_test.dir/progress.make

test/CMakeFiles/p3.04-index-scan_test:
	cd /15-445/bustub-master/build-release/test && ../bin/bustub-sqllogictest /15-445/bustub-master/test/sql/p3.04-index-scan.slt --verbose -d --in-memory

p3.04-index-scan_test: test/CMakeFiles/p3.04-index-scan_test
p3.04-index-scan_test: test/CMakeFiles/p3.04-index-scan_test.dir/build.make

.PHONY : p3.04-index-scan_test

# Rule to build all files generated by this target.
test/CMakeFiles/p3.04-index-scan_test.dir/build: p3.04-index-scan_test

.PHONY : test/CMakeFiles/p3.04-index-scan_test.dir/build

test/CMakeFiles/p3.04-index-scan_test.dir/clean:
	cd /15-445/bustub-master/build-release/test && $(CMAKE_COMMAND) -P CMakeFiles/p3.04-index-scan_test.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/p3.04-index-scan_test.dir/clean

test/CMakeFiles/p3.04-index-scan_test.dir/depend:
	cd /15-445/bustub-master/build-release && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /15-445/bustub-master /15-445/bustub-master/test /15-445/bustub-master/build-release /15-445/bustub-master/build-release/test /15-445/bustub-master/build-release/test/CMakeFiles/p3.04-index-scan_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/p3.04-index-scan_test.dir/depend

