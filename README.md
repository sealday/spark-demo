# spark-demo

spark 直接使用 RDD 以及使用 DataFrame API 的小 demo

## RDD 操作

```java
class GetLength implements Function<String, Integer> {
  public Integer call(String s) { return s.length(); }
}
class Sum implements Function2<Integer, Integer, Integer> {
  public Integer call(Integer a, Integer b) { return a + b; }
}

JavaRDD<String> lines = sc.textFile("data.txt");
JavaRDD<Integer> lineLengths = lines.map(new GetLength());
int totalLength = lineLengths.reduce(new Sum());
```

RDD 操作分类

Transformations 转换，这类操作不会触发产生一个 job
Actions 动作，这类操作将会触发一个 job
Shuffle 洗牌（重分配），这类操作会造成数据迁移、重新分配，会形成一个新的 stage

## 累加器 Accumulators

在 RDD 的操作小函数中，是不推荐直接操作全局状态的，spark 实现了一个
累加器用于某些特殊目的，当我们需要操作全局状态的时候，通过累加器来实现。

## 其他

[任务调度](http://uohzoaix.github.io/studies/2014/09/23/sparkJobScheduling/)
[spark 内存管理](https://www.ibm.com/developerworks/cn/analytics/library/ba-cn-apache-spark-memory-management/index.html)