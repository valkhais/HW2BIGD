import HBase.Hw2HTablesCreator;
import Storm.RecommendationsQualityStormTopology;
import org.junit.Test;

public class stormTest {

    @Test
    public void test() {
        try {
            Hw2HTablesCreator.createTables();
            RecommendationsQualityStormTopology.buildTopology();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}