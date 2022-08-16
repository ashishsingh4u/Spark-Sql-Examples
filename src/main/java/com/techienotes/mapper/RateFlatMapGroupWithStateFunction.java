package com.techienotes.mapper;

import com.techienotes.domain.Rate;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.streaming.GroupState;

import java.util.Iterator;

public class RateFlatMapGroupWithStateFunction implements FlatMapGroupsWithStateFunction<String, Rate, Rate, Rate> {
    @Override
    public Iterator<Rate> call(String s, Iterator<Rate> iterator, GroupState<Rate> groupState) throws Exception {
        Rate next = iterator.next();
        if (groupState.hasTimedOut()) {
            groupState.remove();
        } else if (groupState.exists()) {
            Rate rate = groupState.get();
            if (rate != null) {
                groupState.remove();
            } else {
                groupState.update(next);
                groupState.setTimeoutDuration("15 seconds");
            }
        } else {
            groupState.update(next);
            groupState.setTimeoutDuration("15 seconds");
        }
        return iterator;
    }
}
