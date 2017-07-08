package Storm;

import org.apache.storm.json.simple.JSONObject;

import java.util.*;
import java.util.stream.Collectors;

import static Utils.CommonConstants.*;


public class MaxAvgLiftRecommendBolt extends AbstractRecommendBolt {

    protected List<String> aggregate(List<JSONObject> lift_data) {
        HashMap<String, Double> sum_lift_map = new HashMap<>();
        HashMap<String, Double> num_of_lifts_map = new HashMap<>();

        // iterate over all lift arrays
        for (JSONObject lift_array : lift_data) {
            // iterate over movie,lift pairs in lift array
            for (Object o : lift_array.entrySet()) {
                Map.Entry e = (Map.Entry) o;
                // retrieve movie id and lift
                String movie_id = e.getKey().toString();
                Double lift = Double.valueOf(e.getValue().toString());
                // update lift sum and num of lifts for current movie
                Double curr_sum_lift = sum_lift_map.containsKey(movie_id) ? sum_lift_map.get(movie_id) : 0.0;
                Double curr_num_of_lifts = num_of_lifts_map.containsKey(movie_id) ? num_of_lifts_map.get(movie_id) : 0.0;
                sum_lift_map.put(movie_id, lift + curr_sum_lift);
                num_of_lifts_map.put(movie_id, 1.0 + curr_num_of_lifts);
            }
        }
        // calculate average lift for all movies in the lift arrays
        HashMap<String, Double> avg_lift_map = new HashMap<>();
        for (String movie_id : avg_lift_map.keySet()) {
            avg_lift_map.put(movie_id, sum_lift_map.get(movie_id) / num_of_lifts_map.get(movie_id));
        }
        // sort map by value in descending order using java 8+ api
        HashMap<String, Double> sorted_avg_lift_map = avg_lift_map.entrySet()
                .stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        // collect movies from sun lift map, sorted by sum lift in descending order
        return sorted_avg_lift_map.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toCollection(ArrayList::new));
    }

    protected String getAggregationWindowColumnName() {
        return USERS_TABLE_CF_WINDOW_COL_MAX_AVG_AGG;
    }
}
