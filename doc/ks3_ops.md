# list ks3某个bucket的所有文件url到本地文件
## 开发前准备
### 安装依赖模块

        pip install six


### 安装python ks3sdk
#### 在线安装

        pip install ks3sdk


### 使用方法
  ### linux下执行如下命令:

       nohup python2 ks3_ops.py make_urls <ak> <sk> <bucket_name> <endpoint> &

*常用参数说明*

+ ak：金山云提供的ACCESS KEY ID
+ sk：金山云提供的SECRET KEY ID
+ bucket_name：金山云提供的bucket 名字
+ endpoint：金山云提供的各个Region的域名(例:ks3-cn-beijing.ksyun.com)


*输出文件*
- ./ks3_list_urls 生产签名后url文件列表
- ./ks3_ops.log ks3_ops.py的日志文件
