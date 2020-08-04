## 算子
认知心理学，解决问题的思路其实就时改变问题的状态

问题(初始) ========> 问题(处理中) ========> 问题(解决)

#### 单 Value 转换算子
1.  map
2.  mapPartitions
3.  mapPartitionsWithIndex
4.  flatMap
5.  glom
6.  groupBy     源码调用 groupByKey
7.  filter
8.  sample
9.  distinct    源码调用 reduceByKey
10. coalesce
11. repartition 源码调用 coalesce
12. sortBy  特殊的转换算子  底层出发了执行
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
19. reduceByKey     源码调用combineByKeyWithClassTag    分区内的预聚合功能  ==Combiner==  
20.	groupByKey      源码调用combineByKeyWithClassTag                                   19 20 两个算子的区别 https://www.bilibili.com/video/BV1eC4y1a7UH?p=72
21.	aggregateByKey  源码调用combineByKeyWithClassTag 
22.	foldByKey       源码调用combineByKeyWithClassTag 
23.	combineByKey    源码调用combineByKeyWithClassTag
    P80 讲解 单 Value 算子 pipe, 应该在 P62之后 ** 注意顺序
24.	sortByKey
25.	join
26.	leftOuterJoin rightOuterJoin
27.	cogroup => connect group

####  聚合算子的区别
reduceByKey aggregateByKey foldByKey combineByKey
最终都调用了 combineByKeyWithClassTag 方法

```java
/**
   * :: Experimental ::
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C
   *
   * Users provide three functions:
   *
   *  - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   *  - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   *  - `mergeCombiners`, to combine two C's into a single one.
   *
   * In addition, users can control the partitioning of the output RDD, and whether to perform
   * map-side aggregation (if a mapper can produce multiple items with the same key).
   *
   * @note V and C can be different -- for example, one might group an RDD of type
   * (Int, Int) into an RDD of type (Int, Seq[Int]).
   */
  @Experimental
  def combineByKeyWithClassTag[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null)(implicit ct: ClassTag[C]): RDD[(K, C)] = self.withScope {
    require(mergeCombiners != null, "mergeCombiners must be defined") // required as of Spark 0.9.0
    if (keyClass.isArray) {
      if (mapSideCombine) {
        throw new SparkException("Cannot use map-side combining with array keys.")
      }
      if (partitioner.isInstanceOf[HashPartitioner]) {
        throw new SparkException("HashPartitioner cannot partition array keys.")
      }
    }
    val aggregator = new Aggregator[K, V, C](
      self.context.clean(createCombiner),
      self.context.clean(mergeValue),
      self.context.clean(mergeCombiners))
    if (self.partitioner == Some(partitioner)) {
      self.mapPartitions(iter => {
        val context = TaskContext.get()
        new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
      }, preservesPartitioning = true)
    } else {
      new ShuffledRDD[K, V, C](self, partitioner)
        .setSerializer(serializer)
        .setAggregator(aggregator)
        .setMapSideCombine(mapSideCombine)
    }
  }
```

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

转换算子和行动算子的区别并非十分严格 sortBy???