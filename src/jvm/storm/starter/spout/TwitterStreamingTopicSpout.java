package storm.starter.spout;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

/**
 * Extracts simple topics from the Twitter Streaming API and emits them as words.
 *
 * @author Dan Lynn <dan@fullcontact.com>
 */
public class TwitterStreamingTopicSpout extends BaseRichSpout {
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public void open(Map config, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        TwitterStreamFactory fact = new TwitterStreamFactory(
                new ConfigurationBuilder()
                        .setUser((String) config.get(TWITTER_USERNAME_KEY))
                        .setPassword((String) config.get(TWITTER_PASSWORD_KEY))
                        .build()
        );

        final String[] terms = new String[] { "obama" }; // todo: pull from config, etc..

        TwitterStream stream = fact.getInstance();
        stream.addListener(new QueuingStatusListener());
        log.info("Tracking the following terms: " + Arrays.toString(terms));
        stream.filter(new FilterQuery().track(terms));
    }

    @Override
    public void nextTuple() {
        try {
            String word = queue.take();
            log.info("Emitting word: " + word);
            spoutOutputCollector.emit(new Values(word));
        } catch (InterruptedException e) {
            // drop
        }
    }

    private SpoutOutputCollector spoutOutputCollector;
    private BlockingQueue<String> queue = new LinkedBlockingQueue<String>(1000);
    private static final Logger log = Logger.getLogger(TwitterStreamingTopicSpout.class);
    public static final String TWITTER_USERNAME_KEY = "twitter.username";
    public static final String TWITTER_PASSWORD_KEY = "twitter.password";

    private class QueuingStatusListener implements StatusListener {
        @Override
        public void onStatus(Status status) {
            publish("@" + status.getUser().getName());// author

            for (String topic : extractTopics(status)) {
                publish(topic);
            }

        }

        Pattern splitPattern = Pattern.compile("\\s+");

        /**
         * Simplistic attempt at topic extraction. Returns @username and #hashtags present in a tweet.
         *
         * @param status
         * @return
         */
        public Iterable<String> extractTopics(Status status) {
            final String[] words = splitPattern.split(status.getText());
            List<String> topics = new ArrayList<String>(words.length);
            for (String word : words) {
                if (isValid(word)) {
                    topics.add(word);
                }
            }

            return topics;
        }

        private boolean isValid(String word) {
            return word.startsWith("@") || word.startsWith("#");
        }

        public void publish(final String topic) {
            if (!queue.offer(topic)) {
                log.warn("Queue is full, dropping topic: " + topic);
            }

        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            // do nothing
        }

        @Override
        public void onTrackLimitationNotice(final int numberOfLimitedStatuses) {
            if (numberOfLimitedStatuses > 0) {
                log.warn("Rate-Limited " + String.valueOf(numberOfLimitedStatuses) + " statuses.");
            }

        }

        @Override
        public void onScrubGeo(long userId, long upToStatusId) {
            // do nothing
        }

        @Override
        public void onStallWarning(StallWarning warning) {
            // do nothing
        }

        @Override
        public void onException(Exception ex) {
            // do nothing
        }

    }
}
