### README


#### 编译压测程序

- 环境依赖：aws-cpp-sdk编译环境
- 编译方法：make

#### 配置压测参数

- 压测程序所依赖的配置文件时通过start.sh 脚本动态生成

- start.sh 脚本会执行如下动作：
    - 生成测试文件
    - 生成压测程序所需要的配置文件
    - 执行压测程序
    - 执行dstat 查看客户端系统状态

- 编辑start.sh 填写必要参数，参数分为命令行传输参数和一些静态配置参数
    - 命令行参数：
        - file_size 整数类型，单位为KB，表示压测文件大小
        - max_rps: 整数类型，表示允许客户端压测程序的最大rps
    - 静态配置的参数
        - operator_type:
            - upload:上传
            - download_prepare:下载前上传指定文件，两种<75K和>75K，各自数量1500个即可。下载是遍历下载。
            - download:下载
        - thread_count:线程数量
        - test_time:测试时间
        - file_name:上传文件名字，需放到当前目录
        - file_size:下载文件大小，下载测试需先上传
        - open_log:是否打开log
        - max_rps:限制速度，个/秒
        - save_download_file:下载的文件保存到本地
        - bucket:指定bucket
        - access_key:指定access_key
        - secret_key:指定secret_key

#### 运行压测程序

```
sh start.sh ${file_size} ${max_rps}
```

#### 观察压测程序日志

- rps.log 记录当前压测程序每秒的请求书

```
[hik_test_cpp]# tail -f rps.log
[2017/08/11 03:51:40:0202] rps:208/s
[2017/08/11 03:51:41:0202] rps:214/s
[2017/08/11 03:51:42:0202] rps:213/s
[2017/08/11 03:51:43:0202] rps:200/s
[2017/08/11 03:51:44:0202] rps:213/s
[2017/08/11 03:51:45:0202] rps:211/s
[2017/08/11 03:51:46:0202] rps:233/s
[2017/08/11 03:51:47:0202] rps:205/s
[2017/08/11 03:51:48:0202] rps:226/s
[2017/08/11 03:51:49:0202] rps:221/s
```

- out.log 记录每个请求的开始时间结束时间消耗时间以及线程ID

```
[hik_test_cpp]# tail -f out.log
begin_time:[2017/08/11 03:51:49:0220] end_time:[2017/08/11 03:51:49:0616] key:test_1502394709_353900 used_ms:396 thread_id:140142668031744
begin_time:[2017/08/11 03:51:49:0244] end_time:[2017/08/11 03:51:49:0620] key:test_1502394709_353906 used_ms:376 thread_id:140142269417216
begin_time:[2017/08/11 03:51:49:0049] end_time:[2017/08/11 03:51:49:0624] key:test_1502394709_353859 used_ms:575 thread_id:140142626072320
begin_time:[2017/08/11 03:51:48:0984] end_time:[2017/08/11 03:51:49:0642] key:test_1502394708_353850 used_ms:658 thread_id:140142416275200
begin_time:[2017/08/11 03:51:48:0716] end_time:[2017/08/11 03:51:49:0654] key:test_1502394708_353792 used_ms:938 thread_id:140143140075264
begin_time:[2017/08/11 03:51:49:0214] end_time:[2017/08/11 03:51:49:0655] key:test_1502394709_353895 used_ms:441 thread_id:140142751950592
begin_time:[2017/08/11 03:51:48:0952] end_time:[2017/08/11 03:51:49:0656] key:test_1502394708_353844 used_ms:704 thread_id:140142605092608
begin_time:[2017/08/11 03:51:49:0262] end_time:[2017/08/11 03:51:49:0658] key:test_1502394709_353912 used_ms:396 thread_id:140142709991168
begin_time:[2017/08/11 03:51:49:0250] end_time:[2017/08/11 03:51:49:0665] key:test_1502394709_353909 used_ms:415 thread_id:140142951257856
begin_time:[2017/08/11 03:51:48:0874] end_time:[2017/08/11 03:51:49:0679] key:test_1502394708_353826 used_ms:805 thread_id:140142468724480
^C
```