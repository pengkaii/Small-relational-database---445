# 基于C++的CMU-15445关系型数据库
（线上测试全过）
-----------------

<img src="logo/bustub-whiteborder.svg" alt="BusTub Logo" height="200">

-----------------



## Build构建

```
$ mkdir build
$ cd build
$ cmake ..
$ make
```
## compile编译

```
$ cmake -DCMAKE_BUILD_TYPE=Debug ..
$ make -j`nproc`
```

## Testing测试

```
$ cd build
$ make check-tests
```
