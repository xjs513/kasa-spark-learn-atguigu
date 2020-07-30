## 算子
认知心理学，解决问题的思路其实就时改变问题的状态

问题(初始) ========> 问题(处理中) ========> 问题(解决)

#### 单 Value 转换算子
1.  map
2.  mapPartitions
3.  mapPartitionsWithIndex
4.  flatMap
5.  glom
6.  groupBy
7.  filter
8.  sample
9.  distinct
10. coalesce
11. repartition
12. sortBy
13. pip

#### 双 Value 转换算子
14. intersection 交集
15. union        并集
16. subtract     差集
17. zip          拉链

#### KV-转换算子 
 > 洗牌操作一定会在磁盘落地
 > 一个分区对应一个 task ，
 > 如果上下游两个 task 无需等待，
 > 则可以合并成一个task
 > 如果有 洗牌 过程，则必须要等待，则 task 不能合并成一个任务
18. partitionBy
19. reduceByKey  分区内的预聚合功能  ==Combiner==
20.	groupByKey  19 20 两个算子的区别 https://www.bilibili.com/video/BV1eC4y1a7UH?p=72
21.	aggregateByKey
22.	foldByKey
23.	combineByKey
24.	sortByKey
25.	join
26.	leftOuterJoin
27.	cogroup


###  RDD 行动算子
1)	reduce
2)	collect
3)	count
4)	first
5)	take
6)	takeOrdered
7)	aggregate
8)	fold
9)	countByKey
10)	save相关算子
11)	foreach