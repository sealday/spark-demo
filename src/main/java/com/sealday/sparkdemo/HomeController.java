package com.sealday.sparkdemo;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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

}
