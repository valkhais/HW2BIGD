package Storm;

import HBase.Hw2HTablesCreator;
import Utils.CommonConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;


public class RecommendationsQualityStormTopology {

    private static final String rating_spout = "rating_spout";
    private static final String sum_lift_recommend_bolt = "max_sum_lift_recommend_bolt";
    private static final String avg_lift_recommend_bolt = "max_avg_lift_recommend_bolt";
    private static final String sum_lift_Q_recommend_bolt = "max_sum_lift_Q_recommend_bolt";
    private static final String avg_lift_Q_recommend_bolt = "max_avg_lift_Q_recommend_bolt";
    private static final String sum_lift_Hdfs_bolt = "Max_sum_lift_HDFS_Bolt";
    private static final String avg_lift_Hdfs_bolt = "Max_avg_lift_HDFS_Bolt";
    private static final String recommendations_quality_topology = "recommendations_qulity";


    private static final String hdfs_path = "/hw2_michael";
    private static final String hdfs_url = "hdfs://localhost:9000";

    public static void buildTopology() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");

        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // rotate files when they reach 5MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath(hdfs_path);

        // create an Hdfs bolt, whose job will be to write the output (delta_Q points) to an Hdfs file
        HdfsBolt hdfsbolt = new HdfsBolt()
                .withFsUrl(hdfs_url)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        System.out.println("setting up spout");
        // first step of processing is the ratings spout reading records from ratings.dat in a streaming manner
        builder.setSpout(rating_spout, new RatingsSpout());

        System.out.println("setting up sum lift bolts");
        builder.setBolt(sum_lift_recommend_bolt, new MaxSumLiftRecommendBolt(), 8).fieldsGrouping(rating_spout, new Fields(CommonConstants.USER_ID));
        builder.setBolt(sum_lift_Q_recommend_bolt, new QBolt(), 5).shuffleGrouping(sum_lift_recommend_bolt);
        builder.setBolt(sum_lift_Hdfs_bolt, hdfsbolt, 1).shuffleGrouping(sum_lift_Q_recommend_bolt);

        System.out.println("setting up avg lift bolts");
        builder.setBolt(avg_lift_recommend_bolt, new MaxAvgLiftRecommendBolt(), 8).fieldsGrouping(rating_spout, new Fields(CommonConstants.USER_ID));
        builder.setBolt(avg_lift_Q_recommend_bolt, new QBolt(), 5).shuffleGrouping(avg_lift_recommend_bolt);
        builder.setBolt(avg_lift_Hdfs_bolt, hdfsbolt, 1).shuffleGrouping(avg_lift_Q_recommend_bolt);

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        System.out.println("submitting topology");
        cluster.submitTopology(recommendations_quality_topology, conf, builder.createTopology());
        // we will wait 10 minutes for the cluster to compute the submitted topology, before shutting it down
        System.out.println("WAITING 1 min for the topology to execute");
        Utils.sleep(1000*60);
        System.out.println("Shutting down cluster");
        cluster.shutdown();
        Hw2HTablesCreator.getInstance().cleanup();
    }
}
