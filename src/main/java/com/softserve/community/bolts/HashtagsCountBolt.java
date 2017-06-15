package com.softserve.community.bolts;

import java.util.*;

import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

import dnl.utils.text.table.MapBasedTableModel;
import dnl.utils.text.table.TextTable;
import lombok.extern.log4j.Log4j2;
import twitter4j.HashtagEntity;

/**
 * Created by bogdan on 6/13/2017.
 */
@Log4j2
public class HashtagsCountBolt extends BaseBasicBolt {

    private long top;
    private HashMap<String, Integer> hashtagsCount;

        @Override
    public void prepare(Map stormConf, TopologyContext context) {
        top = (long) stormConf.get("twitter.hashtags.top");
        hashtagsCount = new HashMap<>();
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if (TupleUtils.isTick(tuple)) {
            return;
        }
        HashtagEntity hashtagEntity = (HashtagEntity) tuple.getValueByField("hashtag");
        if (!hashtagsCount.containsKey(hashtagEntity.getText())) {
            hashtagsCount.put(hashtagEntity.getText(), 1);
        } else {
            hashtagsCount.compute(hashtagEntity.getText(), (key, oldValue) -> oldValue + 1);
        }
    }

    @Override
    public void cleanup() {
        log.info("Building result table...");
        List<Map> tableData = new ArrayList<>();
        hashtagsCount.entrySet().stream()
            .sorted((f1, f2) -> Integer.compare(f2.getValue(), f1.getValue())).limit(top)
            .forEach(entry -> tableData
                    .add(ImmutableMap.builder().put("Hashtag", entry.getKey())
                            .put("Count ", entry.getValue()).build()));
        new TextTable(new MapBasedTableModel(tableData)).printTable();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
