package storm.starter;

import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.RankingsReportBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.spout.TwitterStreamingTopicSpout;
import storm.starter.tools.RankableObjectWithFields;
import storm.starter.tools.Rankings;
import storm.starter.util.StormRunner;
import backtype.storm.Config;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 */
public class RollingTopWords {

    private static final int DEFAULT_RUNTIME_IN_SECONDS = 600;
    private static final int TOP_N = 40;

    private final TopologyBuilder builder;
    private final String topologyName;
    private final Config topologyConfig;
    private final int runtimeInSeconds;

    public RollingTopWords() throws InterruptedException {
        builder = new TopologyBuilder();
        topologyName = "slidingWindowCounts";
        topologyConfig = createTopologyConfiguration();
        runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

        wireTopology();
    }

    private static Config createTopologyConfiguration() {
        Config conf = new Config();
        conf.setDebug(true);
        return conf;
    }

    private void wireTopology() throws InterruptedException {
        String spoutId = "wordGenerator";
        String counterId = "counter";
        String intermediateRankerId = "intermediateRanker";
        String totalRankerId = "finalRanker";

        topologyConfig.registerSerialization(Rankings.class);
        topologyConfig.registerSerialization(RankableObjectWithFields.class);


//        builder.setSpout(spoutId, new TestWordSpout(), 5);

        topologyConfig.put(TwitterStreamingTopicSpout.TWITTER_USERNAME_KEY, System.getenv("TWITTER_USERNAME"));
        topologyConfig.put(TwitterStreamingTopicSpout.TWITTER_PASSWORD_KEY, System.getenv("TWITTER_PASSWORD"));
        builder.setSpout(spoutId, new TwitterStreamingTopicSpout(), 1); // limited due to twitter's API

        builder.setBolt(counterId, new RollingCountBolt(300, 1), 4).fieldsGrouping(spoutId, new Fields("word"));
        builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(counterId,
            new Fields("obj"));
        builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);

        builder.setBolt("reporter", new RankingsReportBolt()).globalGrouping(totalRankerId);

    }

    public void run() throws InterruptedException {
        StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
    }

    public static void main(String[] args) throws Exception {
        new RollingTopWords().run();
    }
}
