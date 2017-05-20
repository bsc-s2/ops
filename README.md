# 脚本使用说明

## 简介：

- 上传脚本，可以将本地目录下的文件，上传到白山云存储上。

## 依赖环境

- 操作系统版本： Centos 6.5以上

- python版本：python 2.6 或更高

- python软件包依赖：yaml 和 boto3。<br>
  可以运行`install_dependence.sh`脚本，安装python依赖。

## 安装使用说明

- 运行 `sh install_dependence.sh`, 安装python包依赖，如果确定已经有依赖包，可以略过。

- 运行 `sh install.sh`.

## 脚本列表

| name                                           | desc                               |
| :--                                            | :--                                |
| [upload_directory.py](doc/upload_directory.md) | 将指定目录下的所有文件上传到云存储 |
