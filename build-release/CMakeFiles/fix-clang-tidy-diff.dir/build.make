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

# Utility rule file for fix-clang-tidy-diff.

# Include the progress variables for this target.
include CMakeFiles/fix-clang-tidy-diff.dir/progress.make

CMakeFiles/fix-clang-tidy-diff:
	../build_support/run_clang_tidy.py -clang-tidy-binary /usr/bin/clang-tidy -p /15-445/bustub-master/build-release -clang-apply-replacements-binary /usr/bin/clang-apply-replacements-12 -fix -only-diff

fix-clang-tidy-diff: CMakeFiles/fix-clang-tidy-diff
fix-clang-tidy-diff: CMakeFiles/fix-clang-tidy-diff.dir/build.make

.PHONY : fix-clang-tidy-diff

# Rule to build all files generated by this target.
CMakeFiles/fix-clang-tidy-diff.dir/build: fix-clang-tidy-diff

.PHONY : CMakeFiles/fix-clang-tidy-diff.dir/build

CMakeFiles/fix-clang-tidy-diff.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/fix-clang-tidy-diff.dir/cmake_clean.cmake
.PHONY : CMakeFiles/fix-clang-tidy-diff.dir/clean

CMakeFiles/fix-clang-tidy-diff.dir/depend:
	cd /15-445/bustub-master/build-release && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /15-445/bustub-master /15-445/bustub-master /15-445/bustub-master/build-release /15-445/bustub-master/build-release /15-445/bustub-master/build-release/CMakeFiles/fix-clang-tidy-diff.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/fix-clang-tidy-diff.dir/depend

