package com.example.sparkdemo.controller;

import com.example.sparkdemo.service.sumOfNumbersService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class sumOfNumbersController {

    @Autowired
    private sumOfNumbersService wordCountService;

    @GetMapping("/sumOfNumbers")
    public String getSum() {
        Dataset<Row> x = this.wordCountService.getSum();

        List<String> listOne = x.as(Encoders.STRING()).collectAsList();
        return "SUM OF GIVEN NUMBERS: " + listOne.get(0);

          }


}
