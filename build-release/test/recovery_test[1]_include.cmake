if(EXISTS "/15-445/bustub-master/build-release/test/recovery_test[1]_tests.cmake")
  include("/15-445/bustub-master/build-release/test/recovery_test[1]_tests.cmake")
else()
  add_test(recovery_test_NOT_BUILT recovery_test_NOT_BUILT)
endif()