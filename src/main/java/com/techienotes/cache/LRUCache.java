package com.techienotes.cache;

import com.techienotes.domain.Rate;
import lombok.Data;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class LRUCache implements Serializable {

    private static final Logger logger = Logger.getLogger(LRUCache.class);

    static {
        logger.setLevel(Level.INFO);
    }

    List<Rate> items;
    public LRUCache() {
        items = new ArrayList<>();
    }

    public void display() {
        items.forEach(item -> logger.info(item.toString()));
    }

    public void put(Rate rate) {
        items.add(rate);
    }
}
