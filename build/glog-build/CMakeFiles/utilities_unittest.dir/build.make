# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.26

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /Applications/CMake.app/Contents/bin/cmake

# The command to remove a file.
RM = /Applications/CMake.app/Contents/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /Users/jly/Code/minisql-master

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /Users/jly/Code/minisql-master/build

# Include any dependencies generated for this target.
include glog-build/CMakeFiles/utilities_unittest.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include glog-build/CMakeFiles/utilities_unittest.dir/compiler_depend.make

# Include the progress variables for this target.
include glog-build/CMakeFiles/utilities_unittest.dir/progress.make

# Include the compile flags for this target's objects.
include glog-build/CMakeFiles/utilities_unittest.dir/flags.make

glog-build/CMakeFiles/utilities_unittest.dir/src/utilities_unittest.cc.o: glog-build/CMakeFiles/utilities_unittest.dir/flags.make
glog-build/CMakeFiles/utilities_unittest.dir/src/utilities_unittest.cc.o: /Users/jly/Code/minisql-master/thirdparty/glog/src/utilities_unittest.cc
glog-build/CMakeFiles/utilities_unittest.dir/src/utilities_unittest.cc.o: glog-build/CMakeFiles/utilities_unittest.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/jly/Code/minisql-master/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object glog-build/CMakeFiles/utilities_unittest.dir/src/utilities_unittest.cc.o"
	cd /Users/jly/Code/minisql-master/build/glog-build && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT glog-build/CMakeFiles/utilities_unittest.dir/src/utilities_unittest.cc.o -MF CMakeFiles/utilities_unittest.dir/src/utilities_unittest.cc.o.d -o CMakeFiles/utilities_unittest.dir/src/utilities_unittest.cc.o -c /Users/jly/Code/minisql-master/thirdparty/glog/src/utilities_unittest.cc

glog-build/CMakeFiles/utilities_unittest.dir/src/utilities_unittest.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/utilities_unittest.dir/src/utilities_unittest.cc.i"
	cd /Users/jly/Code/minisql-master/build/glog-build && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/jly/Code/minisql-master/thirdparty/glog/src/utilities_unittest.cc > CMakeFiles/utilities_unittest.dir/src/utilities_unittest.cc.i

glog-build/CMakeFiles/utilities_unittest.dir/src/utilities_unittest.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/utilities_unittest.dir/src/utilities_unittest.cc.s"
	cd /Users/jly/Code/minisql-master/build/glog-build && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/jly/Code/minisql-master/thirdparty/glog/src/utilities_unittest.cc -o CMakeFiles/utilities_unittest.dir/src/utilities_unittest.cc.s

# Object files for target utilities_unittest
utilities_unittest_OBJECTS = \
"CMakeFiles/utilities_unittest.dir/src/utilities_unittest.cc.o"

# External object files for target utilities_unittest
utilities_unittest_EXTERNAL_OBJECTS =

glog-build/utilities_unittest: glog-build/CMakeFiles/utilities_unittest.dir/src/utilities_unittest.cc.o
glog-build/utilities_unittest: glog-build/CMakeFiles/utilities_unittest.dir/build.make
glog-build/utilities_unittest: glog-build/libglogtest.a
glog-build/utilities_unittest: glog-build/CMakeFiles/utilities_unittest.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/Users/jly/Code/minisql-master/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable utilities_unittest"
	cd /Users/jly/Code/minisql-master/build/glog-build && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/utilities_unittest.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
glog-build/CMakeFiles/utilities_unittest.dir/build: glog-build/utilities_unittest
.PHONY : glog-build/CMakeFiles/utilities_unittest.dir/build

glog-build/CMakeFiles/utilities_unittest.dir/clean:
	cd /Users/jly/Code/minisql-master/build/glog-build && $(CMAKE_COMMAND) -P CMakeFiles/utilities_unittest.dir/cmake_clean.cmake
.PHONY : glog-build/CMakeFiles/utilities_unittest.dir/clean

glog-build/CMakeFiles/utilities_unittest.dir/depend:
	cd /Users/jly/Code/minisql-master/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/jly/Code/minisql-master /Users/jly/Code/minisql-master/thirdparty/glog /Users/jly/Code/minisql-master/build /Users/jly/Code/minisql-master/build/glog-build /Users/jly/Code/minisql-master/build/glog-build/CMakeFiles/utilities_unittest.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : glog-build/CMakeFiles/utilities_unittest.dir/depend

