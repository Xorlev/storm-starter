package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.mortbay.util.ajax.JSON;
import storm.starter.tools.Rankable;
import storm.starter.tools.RankableObjectWithFields;
import storm.starter.tools.Rankings;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Reports rankings to an HTTP-based message broker
 *
 * @author Michael Rose <michael@fullcontact.com>
 */
public class RankingsReportBolt extends BaseRichBolt {
    final int ROLLING_SIZE = 60;
    ExecutorService executorService;

    LoadingCache<String, Buffer> bufferCache;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        bufferCache = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .build(new CacheLoader<String, Buffer>() {
                    @Override
                    public Buffer load(String key) throws Exception {
                        return BufferUtils.synchronizedBuffer(new CircularFifoBuffer(ROLLING_SIZE));
                    }
                });

        executorService = Executors.newCachedThreadPool();
    }

    @Override
    public void execute(final Tuple tuple) {
        executorService.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                reportRankings(tuple);
                return null;
            }
        });
    }

    private void reportRankings(Tuple tuple) {List<Rankable> ranks = ((Rankings)tuple.getValue(0)).getRankings();
        System.out.println(ranks);

        List<Map<String, Object>> jsonRanks = new LinkedList<Map<String, Object>>();
        for (Rankable r : ranks) {
            RankableObjectWithFields r2 = (RankableObjectWithFields)r;

            String type = Splitter.on("-").split(tuple.getSourceComponent()).iterator().next();
            String code = r2.getObject().toString();
            Long currentCount = (Long)(r2.getFields().get(0));

            HashMap<String, Object> map = new HashMap<String, Object>();
            map.put("type", type);
            map.put("obj", code);
            map.put("count", r2.getCount());
            map.put("currentCount", currentCount);
            try {
                map.put("percentChange", calculateTrendAndAddElement(bufferCache.get(type+"_"+code), r2.getCount()));
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

            jsonRanks.add(map);
        }

        HttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost("http://localhost:7080/aggregate");

        try {
            post.setEntity(new StringEntity(JSON.toString(jsonRanks)));
            post.setHeader("Content-type", "application/json");
            client.execute(post);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Make this return a double percentage change
     * histogram
     * exponential decay of terms?
     * @return
     */
    public Double calculateTrendAndAddElement(Buffer buffer, Long value) {
        long sum = 0;
        if (buffer.size() == ROLLING_SIZE) {

            for (Object v : buffer) {
                sum += (Long)v;
            }
        }

        System.out.println(sum);

        buffer.add(value);

        if (sum > 0 && value > 0) {
            double weightedOld = sum / (double)ROLLING_SIZE;

            return (value - weightedOld) / weightedOld;
        } else {
            return 0.0;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    static enum Trend {
        UP,
        DOWN,
        FLAT
    }
}