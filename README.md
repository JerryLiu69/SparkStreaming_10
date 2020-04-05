**前言：**

  github 开通账号一年多了，一直都是用来参考别人的代码。那么，现在以来共享一下自己半年前的一个项目的代码吧，这里会做一些处理，仅仅提供大部分思路和代码。

**所用技术栈**

  hadoop-3.1.2
  hive-3.1.2
  spark-2.4.4
  zookeeper-3.4.14
  kafka-2.1.2
  hbase-2.1.7
  azkaban-3.5
  (这些工具都是比Apache较新的版本，因为公司搭建的新的集群用的这些，所以也就在这样一个平台上开发的实时系统（广告实时系统）)
  
**步骤说明**

第一步：flume 将niginx上的日志数据推到kafka中（这里flume需要自己配一个高可用的模式，保证即使挂掉一台niginx，数据能够完整的推到kafka中）

第二步：在第一步前，需要设置好kafka参数，例如数据保存时间策略，创建好topic等信息

第三步：sparkStreaming 消费kafka指定topic消息，结果落到mysql/hdfs/hbase/redis.
（这个过程包括对kafka中数据的清洗，计算日活，留存，新增保存到redis等步骤，具体看代码吧）

**流程：**

    flume  ->  kafka  ->  sparkStreaming  ->  mysql/hbase/hdfs/redis  ->   大屏

**【Scala实现广告实时处理流程。】**


