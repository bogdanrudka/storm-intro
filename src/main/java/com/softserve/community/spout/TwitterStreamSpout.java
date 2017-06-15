package com.softserve.community.spout;

import lombok.extern.log4j.Log4j2;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by bogdan on 6/13/2017.
 */
@Log4j2
public class TwitterStreamSpout extends BaseRichSpout {

    private LinkedBlockingQueue queue;
    private TwitterStream stream;
    private SpoutOutputCollector collector;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;

        ConfigurationBuilder cnfg = new ConfigurationBuilder();
        String customerKey = (String) map.get("twitter.auth.consumer.key");
        String customerSecret = (String) map.get("twitter.auth.consumer.secret");
        String accessToken = (String) map.get("twitter.auth.access.token");
        String accessTokenSecret = (String) map.get("twitter.auth.access.token.secret");

        List<String> filterKeywords = (List) map.get("twitter.filter.hashtags");

        cnfg.setDebugEnabled(true).setOAuthConsumerKey(customerKey)
                .setOAuthConsumerSecret(customerSecret).setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessTokenSecret);

        queue = new LinkedBlockingQueue<>(1000);

        stream = new TwitterStreamFactory(cnfg.build()).getInstance();
        stream.addListener(new StatusAdapter() {
            public void onStatus(Status status) {
                queue.offer(status);
            }
        });

        if (filterKeywords.isEmpty()) {
            stream.sample();
        } else {
            FilterQuery query = new FilterQuery(
                    filterKeywords.toArray(new String[filterKeywords.size()]));
            stream.filter(query);
        }
    }

    public void nextTuple() {
        Status tweet = (Status) queue.poll();
        if (tweet == null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(tweet));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
}
