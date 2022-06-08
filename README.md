# database-project-2022

#### 介绍
2022数据库大程

#### 课程连接
https://git.zju.edu.cn/zjucsdb/minisql

https://www.yuque.com/yingchengjun/pcp6qx/fggii4


## 生成火焰图
sudo perf record -F 99 -p `pidof main` -g -- sleep 30
sudo perf script -i perf.data | stackcollapse-perf.pl --all | flamegraph.pl > flame.svg