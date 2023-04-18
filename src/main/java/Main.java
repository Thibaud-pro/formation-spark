import TD.dataframeTD;
import TD.datasetTD;
import TD.rddTD;
import TP.dataframeTP;
import TP.rddTP;
import conf.SparkSessionManager;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {

        SparkSession spark = SparkSessionManager.getSparkSession();
        SparkConf conf = SparkSessionManager.getSparkConf();
        SparkContext sc = spark.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);

        spark.sparkContext().setLogLevel("ERROR");
        //TD SparkSession
        SparkSessionManager.displayConfigurationOptions(sc);

        //TD RDD
        rddTD.run(spark);

        //TP RDD
        String input = args[0];
        rddTP.wordCount(input, spark);

        //TD Dataframe
        dataframeTD.run(spark);

        //TD Dataset
        datasetTD.run(spark);

        //TP Dataframe
        dataframeTP.run(spark);

        SparkSessionManager.closeSparkSession(spark);
        SparkSessionManager.closeSparkSession(spark);
    }
}
