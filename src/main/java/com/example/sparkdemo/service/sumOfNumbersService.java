package com.example.sparkdemo.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface sumOfNumbersService {
    public Dataset<Row> getSum();

}
