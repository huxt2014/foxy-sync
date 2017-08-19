# foxy-sync --- a foxy man will have backups in difference places

A tool for synchronize files between local file system and aliyun

### 使用方法

```bash
# 对比文件
(foxy_sync) root@raspberrypi:~# foxy-sync /tmp/test  alioss://oss-cn-shanghai.aliyuncs.com/terrence-test
/var/log/foxy_sync/2017-08-19_21:50:29_test>>terrence-test.ts

# 查看执行计划
(foxy_sync) root@raspberrypi:~# foxy-sync /var/log/foxy_sync/2017-08-19_21\:50\:29_test\>\>terrence-test.ts
push   ready    /tmp/test/file00.txt -> file00.txt
push   ready    /tmp/test/subdir/file3.txt -> subdir/file3.txt
remove ready    Learning Python, 5th Edition.pdf
remove ready    file1.txt
remove ready    file2.txt
remove ready    test1/file2.txt
canceled: 0  failed: 0  finished: 0  ready: 6

# 执行
(foxy_sync) root@raspberrypi:~# foxy-sync -i /var/log/foxy_sync/2017-08-19_21\:50\:29_test\>\>terrence-test.ts

# 如果配置文件中配置了endpoint和bucket，可以省略
(foxy_sync) root@raspberrypi:~# foxy-sync /tmp/test  alioss

# 支持子文件夹
(foxy_sync) root@raspberrypi:~# foxy-sync /tmp/test/subdir  alioss://oss-cn-shanghai.aliyuncs.com/terrence-test/subdir

(foxy_sync) root@raspberrypi:~# foxy-sync /tmp/test/subdir  alioss --prefix subdir

```

注：对比文件仅支持md5，仅在Python 3.4 3.5下运行过。暂时不支持从alioss下载。
