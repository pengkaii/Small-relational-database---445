if(EXISTS "/15-445/bustub-master/build-release/test/extendible_hash_table_test[1]_tests.cmake")
  include("/15-445/bustub-master/build-release/test/extendible_hash_table_test[1]_tests.cmake")
else()
  add_test(extendible_hash_table_test_NOT_BUILT extendible_hash_table_test_NOT_BUILT)
endif()
