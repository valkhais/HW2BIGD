package Storm;

import HBase.Hw2HTablesCreator;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.json.simple.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static Utils.CommonConstants.*;

public abstract class AbstractRecommendBolt extends BaseRichBolt {

    //Create instance for OutputCollector which collects and emits tuples to produce output
    private OutputCollector collector;
    private Table lift_table;
    private Table users_table;

    private String aggregation_window_col_name;

    private static TableName lift_table_name = TableName.valueOf(LIFT_TABLE_NAME);
    private static TableName users_table_name = TableName.valueOf(USERS_TABLE_NAME);


    protected abstract List<String> aggregate(List<JSONObject> lift_data);
    protected abstract String getAggregationWindowColumnName();

    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        aggregation_window_col_name = getAggregationWindowColumnName();
        this.collector = collector;

        try {
            lift_table = Hw2HTablesCreator.getInstance().getTable(lift_table_name);
            users_table = Hw2HTablesCreator.getInstance().getTable(users_table_name);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple) {
        String user_id = tuple.getStringByField(USER_ID);
        String movie_id = tuple.getStringByField(MOVIE_ID);
        Double rating = tuple.getDoubleByField(RATING);

        // calculate rating sign for current movie
        String rating_sign;
        if (rating >= MIN_POS_RATING)
            rating_sign = POS;
        else
            rating_sign = NEG;

        // read user's current sliding window
        JSONObject user_w = readFromTableAsJson(
                users_table,
                user_id,
                USERS_TABLE_CF_WINDOW,
                aggregation_window_col_name);

        Double Q_diff = 0.0;

        System.out.println("current window size is " + user_w.size());
        // if user's sliding window is full, retrieve recommendations and update q-diff value
        if (user_w.size() >= WINDOW_SIZE) {
            List<String> ranked_recommendations = aggregate(getLiftDataForUserWindow(user_w));
            System.out.println("ranked recommendations are " + ranked_recommendations);
            System.out.println("current movie is " + movie_id);
            if (ranked_recommendations.contains(movie_id)) {
                // indexes start from 0, ranks from 1
                int rank = 1 + ranked_recommendations.indexOf(movie_id);

                if (rating_sign.equals(POS))
                    Q_diff = (double) (1 / rank);
                else
                    Q_diff = -(double) (1 / rank);
            }
        }

        // update sliding window for processing of next tuple
        user_w = getUpdatedSlidingWindow(user_w, movie_id, rating_sign);
        writeToTable(user_w,
                users_table,
                user_id,
                USERS_TABLE_CF_WINDOW,
                aggregation_window_col_name);
        System.out.println("Calculated Q_diff is " + Q_diff);
        // emit calculated Q for current tuple to QBolt
        collector.emit(new Values(Q_diff));
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Q));
    }

    private List<JSONObject> getLiftDataForUserWindow(JSONObject user_w) {
        List<JSONObject> lift_data = new ArrayList<>();

        for (Integer i = 0; i < WINDOW_SIZE; ++i) {
            // get user window as JSONObject
            LinkedTreeMap rated_movie_map = (LinkedTreeMap) user_w.get(i.toString());
            JSONObject rated_movie = new JSONObject();
            for (Object o : rated_movie_map.entrySet())
            {
                Map.Entry e = (Map.Entry) o;
                rated_movie.put(e.getKey(),e.getValue());
            }
            // retrieve the corresponding lift data from lift table, for the current rated movie (from window)
            JSONObject lift_array = readFromTableAsJson(
                    lift_table,
                    rated_movie,
                    LIFT_TABLE_CF_LIFT,
                    LIFT_TABLE_CF_LIFT_COL_TOP_K);

            lift_data.add(lift_array);
        }
        System.out.println("got user window: " + user_w);
        System.out.println("Calculated lift_data is: " + lift_data);

        return lift_data;
    }

    private JSONObject getUpdatedSlidingWindow(JSONObject curr_window, String movie, String rating_sign) {
        JSONObject new_pair = new JSONObject();
        new_pair.put(movie, rating_sign);

        if (curr_window.size() < WINDOW_SIZE) {
            curr_window.put(curr_window.size(), new_pair);
            return curr_window;
        }
        // create a new window, simulating a "slide" as a result of removing the oldest pair
        JSONObject new_window = new JSONObject();
        for (Integer i = 1; i < WINDOW_SIZE; ++i) {
            new_window.put(i - 1, curr_window.get(i.toString()));
        }
        new_window.put(WINDOW_SIZE - 1, new_pair);
        return new_window;
    }

    private void writeToTable(Object value, Table table, Object key, Object cf, Object col) {
        try {
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes(cf.toString()), Bytes.toBytes(col.toString()), Bytes.toBytes(value.toString()));
            table.put(put);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private JSONObject getJsonFromBytes(byte[] bytes) throws IOException {
        // return an empty Json object if bytes data is null
        if (bytes == null)
            return new JSONObject();

        Gson gson = new Gson();
        return gson.fromJson(new String(bytes), JSONObject.class);

    }

    private JSONObject readFromTableAsJson(Table table, Object key, Object cf, Object col) {
        try {
            Get get = new Get(Bytes.toBytes(key.toString()));
            return getJsonFromBytes(table.get(get).getValue(Bytes.toBytes(cf.toString()),Bytes.toBytes(col.toString())));

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
