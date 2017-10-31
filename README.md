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

## 其他

[任务调度](http://uohzoaix.github.io/studies/2014/09/23/sparkJobScheduling/)
[spark 内存管理](https://www.ibm.com/developerworks/cn/analytics/library/ba-cn-apache-spark-memory-management/index.html)