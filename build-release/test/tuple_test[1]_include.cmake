if(EXISTS "/15-445/bustub-master/build-release/test/tuple_test[1]_tests.cmake")
  include("/15-445/bustub-master/build-release/test/tuple_test[1]_tests.cmake")
else()
  add_test(tuple_test_NOT_BUILT tuple_test_NOT_BUILT)
endif()
