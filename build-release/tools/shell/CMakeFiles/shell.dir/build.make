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
include tools/shell/CMakeFiles/shell.dir/depend.make

# Include the progress variables for this target.
include tools/shell/CMakeFiles/shell.dir/progress.make

# Include the compile flags for this target's objects.
include tools/shell/CMakeFiles/shell.dir/flags.make

tools/shell/CMakeFiles/shell.dir/shell.cpp.o: tools/shell/CMakeFiles/shell.dir/flags.make
tools/shell/CMakeFiles/shell.dir/shell.cpp.o: ../tools/shell/shell.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/15-445/bustub-master/build-release/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object tools/shell/CMakeFiles/shell.dir/shell.cpp.o"
	cd /15-445/bustub-master/build-release/tools/shell && /usr/bin/clang++-12  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/shell.dir/shell.cpp.o -c /15-445/bustub-master/tools/shell/shell.cpp

tools/shell/CMakeFiles/shell.dir/shell.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/shell.dir/shell.cpp.i"
	cd /15-445/bustub-master/build-release/tools/shell && /usr/bin/clang++-12 $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /15-445/bustub-master/tools/shell/shell.cpp > CMakeFiles/shell.dir/shell.cpp.i

tools/shell/CMakeFiles/shell.dir/shell.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/shell.dir/shell.cpp.s"
	cd /15-445/bustub-master/build-release/tools/shell && /usr/bin/clang++-12 $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /15-445/bustub-master/tools/shell/shell.cpp -o CMakeFiles/shell.dir/shell.cpp.s

# Object files for target shell
shell_OBJECTS = \
"CMakeFiles/shell.dir/shell.cpp.o"

# External object files for target shell
shell_EXTERNAL_OBJECTS =

bin/bustub-shell: tools/shell/CMakeFiles/shell.dir/shell.cpp.o
bin/bustub-shell: tools/shell/CMakeFiles/shell.dir/build.make
bin/bustub-shell: lib/libbustub.a
bin/bustub-shell: lib/libbustub_linenoise.a
bin/bustub-shell: lib/libutf8proc.a
bin/bustub-shell: lib/libbustub_murmur3.a
bin/bustub-shell: lib/libduckdb_pg_query.a
bin/bustub-shell: lib/libfmt.a
bin/bustub-shell: lib/libfort.a
bin/bustub-shell: tools/shell/CMakeFiles/shell.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/15-445/bustub-master/build-release/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable ../../bin/bustub-shell"
	cd /15-445/bustub-master/build-release/tools/shell && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/shell.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
tools/shell/CMakeFiles/shell.dir/build: bin/bustub-shell

.PHONY : tools/shell/CMakeFiles/shell.dir/build

tools/shell/CMakeFiles/shell.dir/clean:
	cd /15-445/bustub-master/build-release/tools/shell && $(CMAKE_COMMAND) -P CMakeFiles/shell.dir/cmake_clean.cmake
.PHONY : tools/shell/CMakeFiles/shell.dir/clean

tools/shell/CMakeFiles/shell.dir/depend:
	cd /15-445/bustub-master/build-release && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /15-445/bustub-master /15-445/bustub-master/tools/shell /15-445/bustub-master/build-release /15-445/bustub-master/build-release/tools/shell /15-445/bustub-master/build-release/tools/shell/CMakeFiles/shell.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : tools/shell/CMakeFiles/shell.dir/depend

