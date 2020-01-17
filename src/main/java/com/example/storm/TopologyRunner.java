package com.example.storm;

import com.example.storm.bolt.FilterZeroBolt;
import com.example.storm.bolt.PrintBolt;
import com.example.storm.bolt.WindowedAggregateSumBolt;
import com.example.storm.spout.RandomIntegerSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

public class TopologyRunner {
    public static void main(String[] args) throws Exception {
        runTopology();
    }

    private static void runTopology() throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("randIntSpout", new RandomIntegerSpout());
        topologyBuilder.setBolt("filterZeroBolt", new FilterZeroBolt()).shuffleGrouping("randIntSpout");
        topologyBuilder.setBolt("windowSumBolt",
                new WindowedAggregateSumBolt()
                        .withLag(BaseWindowedBolt.Duration.seconds(1))
                        .withWindow(BaseWindowedBolt.Duration.seconds(5))
                        .withTimestampField("timestamp")
        ).shuffleGrouping("filterZeroBolt");
        topologyBuilder.setBolt("printBolt", new PrintBolt()).shuffleGrouping("windowSumBolt");
        Config config = new Config();
        config.setDebug(false);
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("SampleRandSum", config, topologyBuilder.createTopology());

    }
}
