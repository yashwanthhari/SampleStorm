package com.example.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomIntegerSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private Random random;

    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.random = new Random();
    }

    public void nextTuple() {
        Utils.sleep(1000);
        spoutOutputCollector.emit(new Values(random.nextInt(100), System.currentTimeMillis()));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("rand-int", "timestamp"));
    }
}
