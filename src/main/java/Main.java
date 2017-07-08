import HBase.Hw2HTablesCreator;
import Storm.RecommendationsQualityStormTopology;

import java.io.IOException;

public class Main {

    public static void main (String[] args) throws IOException
    {
        Hw2HTablesCreator.getInstance().createTables();
        try {
            RecommendationsQualityStormTopology.buildTopology();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}