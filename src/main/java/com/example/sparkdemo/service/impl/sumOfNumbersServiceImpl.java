package com.example.sparkdemo.service.impl;

import com.example.sparkdemo.model.number;

import com.example.sparkdemo.service.sumOfNumbersService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class sumOfNumbersServiceImpl implements sumOfNumbersService {

    @Autowired
    private SparkSession sparkSession;




    @Override


    public Dataset<Row> getSum(){
        String input = "12 123 123 43 23 22";
        String [] _numbers = input.split(" ");
        List<number> numbers = Arrays.stream(_numbers).map(number::new).collect(Collectors.toList());
        Dataset<Row> dataFrame = sparkSession.createDataFrame(numbers, number.class);
        dataFrame.show();

        dataFrame.createOrReplaceTempView("numbers");

        dataFrame = sparkSession
                .sql("SELECT cast(number as float) number FROM numbers");

        Dataset<Row> result = sparkSession.sql("SELECT sum(number) as SUM FROM numbers");
        return result;

    }

}
