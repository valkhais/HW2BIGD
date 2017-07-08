package Storm;

import Utils.CommonConstants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;


public class QBolt extends BaseRichBolt {

    private Integer num_of_recommendations = 0;
    private Double delta_Q = 0.0;
    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        System.out.println("Previous delta_Q is " + delta_Q);
        delta_Q += tuple.getDoubleByField(CommonConstants.Q);
        System.out.println("New delta_Q is " + delta_Q);

        if(num_of_recommendations++ == 1000){
            num_of_recommendations = 0;
            System.out.println("Emitted delta_Q is " + delta_Q);
            collector.emit(new Values(delta_Q));
            delta_Q = 0.0;
        }
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(CommonConstants.Q));
    }



}
