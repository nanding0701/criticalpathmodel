###set -x

compileopt=$1
if [ "$compileopt"x == "1"x ];then
    echo "Compiling..."
    g++ CriticalPathModel.cc -DDEBUG_2 -std=c++17 -o commutimedebug_CriticalPathModel && 
    g++ CriticalPathModel.cc -DDEBUG_1 -std=c++17 -o buildgraphdebug_CriticalPathModel &&
    g++ CriticalPathModel.cc -DDEBUG_0 -std=c++17 -o clean_CriticalPathModel &&
    g++ CriticalPathModel.cc -std=c++17 -o multithread_CriticalPathModel && 
    echo "Compiling done"
fi

#for i in {1..10};
#do
##./multithread_CriticalPathModel s1_16x16.csv 16 16
###./clean_CriticalPathModel test.csv 2 2
./clean_CriticalPathModel s1_16X16.csv 16 16
#done
#./clean_CriticalPathModel L_64ranks.csv 16 16  >> test.log


