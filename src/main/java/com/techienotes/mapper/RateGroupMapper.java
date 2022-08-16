package com.techienotes.mapper;

import com.techienotes.domain.Rate;
import org.apache.spark.api.java.function.MapFunction;

public class RateGroupMapper implements MapFunction<Rate, String> {
    @Override
    public String call(Rate rate) throws Exception {
        return rate.getStock();
    }
}
