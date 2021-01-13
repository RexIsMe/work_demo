package com.rex.demo.study.demo.driver.batch.partition;

/**
 * 介绍：
 * 1、Rebalance：对数据集进行再平衡，重分区，消除数据倾斜
 * 2、Hash-Partition：根据指定key的哈希值对数据集进行分区
 * 3、partitionByHash()
 * 4、Range-Partition：根据指定的key对数据集进行范围分区
 * 5、.partitionByRange()
 * 6、Custom Partitioning：自定义分区规则
 * 7、自定义分区需要实现Partitioner接口
 * 8、partitionCustom(partitioner, "someKey")
 * 9、或者partitionCustom(partitioner, 0)
 *
 *
 * @Author li zhiqang
 * @create 2021/1/11
 */
public class Common
{
}
