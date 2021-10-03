cmake_minimum_required(VERSION 3.13)

project(MergeableCollection)

find_package(ROOT 6.24 REQUIRED)

set(CMAKE_CXX_STANDARD 17)

add_library(MergeableCollection SHARED)

target_sources(MergeableCollection PRIVATE MergeableCollection.cxx)

target_link_libraries(MergeableCollection PUBLIC ROOT::Core ROOT::RIO ROOT::Hist)
target_include_directories(MergeableCollection PUBLIC .)

target_compile_definitions(MergeableCollection PRIVATE MERGEABLE_COLLECTION_STANDALONE)

root_generate_dictionary(G__MergeableCollection MergeableCollection.h MODULE MergeableCollection LINKDEF MergeableCollectionLinkDef.h)

