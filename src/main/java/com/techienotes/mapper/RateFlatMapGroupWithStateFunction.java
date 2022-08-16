package com.techienotes.mapper;

import com.clearspring.analytics.util.Lists;
import com.techienotes.cache.LRUCache;
import com.techienotes.domain.Rate;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.streaming.GroupState;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RateFlatMapGroupWithStateFunction implements FlatMapGroupsWithStateFunction<String, Rate, LRUCache, Rate> {

    @Override
    public Iterator<Rate> call(String s, Iterator<Rate> iterator, GroupState<LRUCache> groupState) throws Exception {
        LRUCache cache;
        if (groupState.hasTimedOut()) {
            groupState.remove();
            return iterator;
        } else if (groupState.exists()) {
            cache = groupState.get();
        } else {
            cache = new LRUCache();
        }

        List<Rate> items = new ArrayList<>();
        iterator.forEachRemaining(item -> {
            items.add(item);
            cache.put(item);
        });
        cache.display();
        groupState.update(cache);
        groupState.setTimeoutDuration("15 seconds");

        return items.iterator();
    }
}
