if(EXISTS "/15-445/bustub-master/build-release/test/lock_manager_compability_test[1]_tests.cmake")
  include("/15-445/bustub-master/build-release/test/lock_manager_compability_test[1]_tests.cmake")
else()
  add_test(lock_manager_compability_test_NOT_BUILT lock_manager_compability_test_NOT_BUILT)
endif()
