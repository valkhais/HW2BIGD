import HBase.Hw2HTablesCreator;
import Storm.RecommendationsQualityStormTopology;
import org.junit.Test;

public class stormTest {

    @Test
    public void test() {
        try {
            System.out.println("calling create tables");
            Hw2HTablesCreator.getInstance().createTables();
            System.out.println("calling build topology");
            RecommendationsQualityStormTopology.buildTopology();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}