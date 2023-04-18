package conf;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SparkSessionManagerTest {

    private SparkSession sparkSession;

    @BeforeEach
    public void setup() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Test App")
                .setMaster("local[*]");

        sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }

    @Test
    public void testClose() {
        // Test that the SparkSession is open before closing
        Assertions.assertFalse(sparkSession.sparkContext().isStopped());

        // Call the close method
        SparkSessionManager.closeSparkSession(sparkSession);

        // Test that the SparkSession is closed after calling close()
        Assertions.assertTrue(sparkSession.sparkContext().isStopped());
    }

    @Test
    public void testGetSparkSession() {
        // Call the getSparkSession method to obtain an instance of SparkSession
        SparkSession sparkSession = SparkSessionManager.getSparkSession();

        // Verify that the SparkSession is not null
        Assertions.assertNotNull(sparkSession);

        // Verify that the SparkSession is not already closed
        Assertions.assertFalse(sparkSession.sparkContext().isStopped());

        // Verify that the SparkSession has the correct app name
        Assertions.assertEquals("Formation-Spark", sparkSession.conf().get("spark.app.name"));

        // Verify that the SparkSession has the correct master URL
        Assertions.assertEquals("local[4]", sparkSession.conf().get("spark.master"));

        // Verify that the SparkSession event logging is enabled
        Assertions.assertEquals("true", sparkSession.conf().get("spark.eventLog.enabled"));
    }

    @AfterEach
    public void tearDown() {
        // Make sure the SparkSession is closed after each test
        if (sparkSession != null) {
            sparkSession.close();
        }
    }
}
