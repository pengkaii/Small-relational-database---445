if(EXISTS "/15-445/bustub-master/build-release/test/b_plus_tree_contention_test[1]_tests.cmake")
  include("/15-445/bustub-master/build-release/test/b_plus_tree_contention_test[1]_tests.cmake")
else()
  add_test(b_plus_tree_contention_test_NOT_BUILT b_plus_tree_contention_test_NOT_BUILT)
endif()
