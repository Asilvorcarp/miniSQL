# database-project-2022

#### 介绍
2022数据库大程

#### 课程连接
https://git.zju.edu.cn/zjucsdb/minisql

https://www.yuque.com/yingchengjun/pcp6qx/fggii4



# YJ-个人详细设计报告

- [ ] 对着commit记录写
- [ ] 测试代码的设计说明

## **SQL EXECUTOR**

### PrimaryKey的实现

CreateTable时自动为PrimaryKey的若干列建立B+树索引 (名称为...)

并且会将PrimaryKey各列的Index存储在TableMeta的属性PrimaryKeyIndexs中

### Unique的实现

CreateTable时自动为Unique列建立B+树索引 (名称为...)

### 关于CreateIndex

能否CreateIndex的标准是：是否实际上有重复 (而非列属性标记为Unique)

如果实际上没有重复，就允许CreateIndex，并且在CreateIndex之后会将该列标记为Unique

### 加速的实现

在ExecuteEngine下添加了canAccelerate函数：

   *  输入: whereNode, table_info and CatalogManager.
   *  输出: (如果成功实施加速) result rows.
   *  返回值: 
      *  0b000: 未实施任何加速。
         *  这意味着后续处理where语句需要遍历整个表。

      *  0b001: 实施了部分加速，后续仍需过滤(filter)。
         *  对应的情形 //todo 
            *  例如 where id = 1 and name = "yj" 其中id上有索引，name上没有索引
               这时函数会返回所有id = 1的Rowid，并且通过返回值提示后续需要再过滤(即筛选出其中name = "yj"的Rowid)

      *  0b010: 实施了全部加速，后续不再需要过滤。
         *  对应 //todo


(使用二进制返回值是希望后续的分支判断只需要位运算，从而加速)

之后ExecuteEngine中select等函数只需

## 性能调优

生成火焰图

sudo perf record -F 99 -p `pidof main` -g -- sleep 30
sudo perf script -i perf.data | stackcollapse-perf.pl --all | flamegraph.pl > flame.svg