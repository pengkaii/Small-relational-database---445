if(EXISTS "/15-445/bustub-master/build-release/test/buffer_pool_manager_instance_test[1]_tests.cmake")
  include("/15-445/bustub-master/build-release/test/buffer_pool_manager_instance_test[1]_tests.cmake")
else()
  add_test(buffer_pool_manager_instance_test_NOT_BUILT buffer_pool_manager_instance_test_NOT_BUILT)
endif()
