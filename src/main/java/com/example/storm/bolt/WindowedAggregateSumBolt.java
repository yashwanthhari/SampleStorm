package com.example.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class WindowedAggregateSumBolt extends BaseWindowedBolt {

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("windowSum", "startTimestamp", "endTimestamp"));
    }

    public void execute(TupleWindow tupleWindow) {
        List<Tuple> tupleList = tupleWindow.get();
        tupleList.sort(Comparator.comparing(this::getTimeStamp));
        long sum = tupleList.stream()
                .mapToInt(tuple -> tuple.getIntegerByField("non-zero-int"))
                .sum();
        long start = getTimeStamp(tupleList.get(0));
        long end = getTimeStamp(tupleList.get(tupleList.size() - 1));
        Values values = new Values(sum, start, end);
        outputCollector.emit(values);
    }

    private long getTimeStamp(Tuple tuple) {
        return tuple.getLongByField("timestamp");
    }
}
