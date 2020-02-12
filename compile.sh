set -x

g++ CriticalPathModel.cc -DDEBUG_2 -std=c++17 -o commutimedebug_CriticalPathModel
g++ CriticalPathModel.cc -DDEBUG_1 -std=c++17 -o buildgraphdebug_CriticalPathModel
g++ CriticalPathModel.cc -DDEBUG_0 -std=c++17 -o clean_CriticalPathModel


