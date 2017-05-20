# 上传本地目录到云端 `upload_directory.py`

使用方法:

- 在conf目录下，根据需求修改`upload_directory.yaml`文件中的配置。

- 在bin目录下，运行 `python upload_directory.py`进行文件上传。

## 配置文件说明：

`upload_directory.yaml` 描述了将目录上传到存储中的权限, 目标位置, 执行实现,
并发程度等配置.

### 1. ACCESS_KEY字段：

- 用户访问云存储需要的access key。

### 2. SECRET_KEY字段：

- 用户访问云存储需要的secret key。

### 3. BUCKET_NAME字段：

- 用户将文件上传到的bucket名字。

### 4. KEY_PREFIX字段：

- 上传文件会以上传目录下的路径，作为文件的key。<br>
  比如上传的文件目录是`/upload`。<br>
  上传的文件 `/upload/movie/2017/5/16/hello_world.mp4`，上传到云存储，<br>
  key将是：`/movie/2017/5/16/hello_world.mp4`

- 如果指定KEY_PREFIX字段，比如`KEY_PREFIX='dreamer'` <br>
  在上面的情况下，上传的key将是：`/dreamer/movie/2017/5/16/hello_world.mp4`

### 5. FILE_ACL字段：

- 指定文件上传后的访问权限

    -    `private`
    -    `public-read`
    -    `public-read-write`
    -    `authenticated-read`

具体请参考云存储文件的acl的说明。

### 6. DATA_DIR字段：

- 上传文件的目录，注意该目录不会存在于key的名字中。

### 7. LOG_DIR字段：

- 日志存放的目录，目录下有两个日志文件：

    - upload-log-for 开头的log文件。
    - upload-progress-for 开头的log文件。

具体在日志说明章节讲解。

### 8. CLEAR_FILES字段：

- `DATA_DIR`下的文件，在上传完成后，是否清除文件。

### 9. THREADS_NUM_FOR_DIR字段：

- 处理目录的线程数，可以并发的对多个目录下的文件，进行上传。<br>
  如果`DATA_DIR`目录下的子目录比较多，可以适当调大并发数。

### 10. THREADS_NUM_FOR_FILE字段：

- 处理目录下文件的线程数，可以并发的对目录下的文件，进行并发上传。<br>
  如果`DATA_DIR`目录下的文件比较多，可以适当调大并发数。

- 调节`THREADS_NUM_FOR_DIR`和`THREADS_NUM_FOR_FILE`，需要注意，<br>
如果`THREADS_NUM_FOR_DIR`是3，`THREADS_NUM_FOR_FILE`是10，<br>
在目录数和文件数都很多的情况下，同时会有 `3*10`个线程在运行。

- 所以根据自己的业务情况，调节合适的参数，才能发挥上传脚本的最佳性能。

### 11. ENABLE_BANDWIDTH字段：

- 是否启用带宽限制，启用带宽限制，上传的时候，会对上传进行限速。

### 12. BANDWIDTH字段：

- 限制的上传速度，单位是为Mb。

### 13. REPORT_INTERVAL字段：

- 汇报的间隔，单位是秒。<br>
  定期对上传的进度进行汇报，结果会输出到upload-log-for开头的log文件和标准输出中。

### 14. RUN_FOREVER字段：

- `true`, 持续运行该脚本，`DATA_DIR`目录下的文件上传完成后，脚本不退出，等待加入新文件，持续上传。

- `false`, 脚本将`DATA_DIR`目录下的文件上传完成后，脚本退出。

### 15. ENABLE_SCHEDULE字段：

- 是否启用定期运行功能，如果启用，可以指定每天某个时间段，脚本上传文件。

### 16. SCHEDULE_START、SCHEDULE_STOP字段：

- 脚本启动和停止的时间, 比如：

    - `SCHEDULE_START: '01:25'`
    - `SCHEDULE_STOP: '23:30'`


## 日志文件说明：

`LOG_DIR`指定的目录下，有两个日志文件：

-   upload-log-for 开头的log文件，
-   upload-progress-for 开头的log文件。

### upload-log-for*

文件用于记录上传的日志，比如上传的进度，上传的错误信息等。

### upload-progress-for*

文件记录成功上传每个文件的信息。

比如：

```
/root/upload_directory/test.txt   test.txt   "4f6885af8bc07006f5dc8e4b9e7dc8f9"  15911   20170516T124335Z
/root/upload_directory/total.txt  total.txt  "b42ba2b8a7ffb5222c5bc7ab647843bc"  1353936  20170516T124336Z
```

说明：

- 第一列: 上传的文件的路径。
- 第二列: 上传到云存储上的key名。
- 第三列: 文件的MD5校验值。
- 第四列: 文件的大小。
- 第五列: 文件上传的时间, 时间值是格林尼治标准时间。
