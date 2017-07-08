import HBase.Hw2HTablesCreator;
import Storm.RecommendationsQualityStormTopology;
import org.junit.Test;
import java.io.IOException;

public class HBaseTests {

    @Test
    public void testTopology() throws IOException {
        Hw2HTablesCreator.createTables();
        try {
            RecommendationsQualityStormTopology.buildTopology();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
