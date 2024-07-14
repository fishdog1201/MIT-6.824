message("Start build util!")
add_library(mapReduce SHARED mapReduceFun.cpp)
target_include_directories(mapReduce PUBLIC .)