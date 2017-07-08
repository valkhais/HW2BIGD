package Storm;

import Utils.CommonConstants;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;


public class RatingsSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private BufferedReader ratings_file;

    private String RATINGS_LINE_DELIMITER = "::";

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        try {
            ratings_file = new BufferedReader(new FileReader(CommonConstants.RATINGS_PATH));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void nextTuple() {

        String line = null;
        try {
            line = ratings_file.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (line != null)
        {
            String[] splitted_line = line.split(RATINGS_LINE_DELIMITER);
            String user_id = splitted_line[0];
            String movie_id = splitted_line[1];
            Double rating = Double.valueOf(splitted_line[2]);
            collector.emit(new Values(user_id, movie_id, rating));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(CommonConstants.USER_ID, CommonConstants.MOVIE_ID, CommonConstants.RATING));

    }
}
