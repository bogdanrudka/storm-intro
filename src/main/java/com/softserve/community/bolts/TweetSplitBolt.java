package com.softserve.community.bolts;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import lombok.extern.log4j.Log4j2;
import org.apache.storm.utils.TupleUtils;
import twitter4j.HashtagEntity;
import twitter4j.Status;

/**
 * Created by bogdan on 6/14/2017.
 */
@Log4j2
public class TweetSplitBolt extends BaseBasicBolt {

    private Set<String> filterKeywords;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            if (TupleUtils.isTick(tuple)) {
                    return;
            }
            Status tweet = (Status) tuple.getValueByField("tweet");
            log.info(tweet.getText());
            for (HashtagEntity entity : tweet.getHashtagEntities()) {
                    basicOutputCollector.emit(new Values(entity));
            }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("hashtag"));
    }
}
