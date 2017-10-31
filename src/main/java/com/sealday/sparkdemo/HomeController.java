package com.sealday.sparkdemo;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;


@RestController
public class HomeController {

    private final JavaSparkContext jsc;
    private final SparkSession spark;

    @Autowired
    public HomeController(JavaSparkContext jsc, SparkSession spark) {
        this.jsc = jsc;
        this.spark = spark;
    }

    @RequestMapping("/")
    String home() {
        return "home";
    }

    @RequestMapping("/examples/word_count")
    List<?> wordCount(){
        JavaRDD<String> textFile = jsc.textFile(getClass().getResource("/data/words.txt").getFile());
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        return counts.collect();
    }

    @RequestMapping("/examples/pi")
    double piEstimation(@RequestParam(defaultValue = "10", required = false) int num) {
        List<Integer> l = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            l.add(i);
        }

        long count = jsc.parallelize(l).filter(i -> {
            double x = Math.random();
            double y = Math.random();
            return x * x + y * y < 1;
        }).count();
        return 4.0 * count / num;
    }

    @RequestMapping("/examples/pi_multi")
    double piEstimationMulti(@RequestParam(defaultValue = "10", required = false) int num) {
        List<Integer> l = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            l.add(i);
        }

        l.parallelStream().forEach(i -> piEstimation(10000000));
        return 1;
    }

    @RequestMapping("/examples/text_search")
    long textSearch(@RequestParam(defaultValue = "vim", required = false) String keyword) {
        JavaRDD<String> textFile = jsc.textFile(getClass().getResource("/data/gui.c").getFile());
        JavaRDD<Row> rowRDD = textFile.map(RowFactory::create);
        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("line", DataTypes.StringType, true)
        );
        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema);
        Dataset<Row> lines = df.filter(col("line").like("%" + keyword + "%"));
        return lines.count();
    }

    @RequestMapping("/examples/simple_op")
    long simpleDataOp() {
        String url =
                "jdbc:mysql://127.0.0.1/test?user=seal&password=..xiao";
        Dataset<Row> df = spark.sqlContext()
                .read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", "people")
                .load();

        df.printSchema();

        Dataset<Row> countsByAge = df.groupBy("age").count();
        countsByAge.show();
        return 0;
    }
}
