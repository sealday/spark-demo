package com.sealday.sparkdemo;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class SparkService {

    @Bean
    SparkSession spark() {
        return SparkSession
                .builder()
                .appName("Spark Demo")
                // 利用的核心个数，可以换成具体的核心数字 比如 local[4] 四个核心
                .master("local[*]")
                .getOrCreate();
    }

    @Bean
    JavaSparkContext jsc(SparkSession spark) {
        return new JavaSparkContext(spark.sparkContext());
    }

}
