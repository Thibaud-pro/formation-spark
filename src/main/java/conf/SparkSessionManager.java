package conf;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public final class SparkSessionManager {
    //Le mot-clé final pour empêcher l'héritage de cette classe
    private static SparkSession spark;
    private static SparkConf conf;
    static final String appName = "Formation-Spark";
    static final Integer numCore = 4;
    static final String driverMem = "2g";

    private SparkSessionManager() {
        // constructeur privé pour empêcher la création d'instances de la classe.
        // Cela est important car les méthodes utilitaires ne nécessitent pas d'instances pour être appelées.
    }

    //Les méthodes static en Java sont souvent utilisées pour accéder à des méthodes ou des variables de classe,
    // utiliser des méthodes utilitaires ou créer des instances de classe

    public static SparkSession getSparkSession() {
        if (spark == null) {
            spark = SparkSession
                    .builder()
                    .config(getSparkConf())
                    .config("spark.eventLog.enabled",true)
                    .getOrCreate();
        }
        return spark;
    }

    public static SparkConf getSparkConf() {
        if (conf == null) {
            conf = new SparkConf()
                    .setMaster(String.format("local[%d]", numCore))
                    .setAppName(appName)
                    .set("spark.driver.memory", driverMem);
        }
        return conf;
    }

    public static void displayConfigurationOptions(Object o) {
        Tuple2<String, String>[] confMap;
        if (o instanceof SparkSession) {
            confMap = ((SparkSession) o).sparkContext().conf().getAll();
        } else if (o instanceof SparkContext) {
            confMap = ((SparkContext) o).conf().getAll();
        } else if (o instanceof JavaSparkContext) {
            confMap = ((JavaSparkContext) o).getConf().getAll();
        } else {
            throw new IllegalArgumentException("Unsupported value type: " + o.getClass().getName());
        }
        for (Tuple2<String, String> tuple : confMap) {
            System.out.println(tuple._1 + ": " + tuple._2);
        }
    }

    public static void closeSparkSession(SparkSession spark) {
        if (!spark.sparkContext().isStopped()) {
            spark.close();
            System.out.println("SparkSession closed successfully.");
        } else {
            System.out.println("SparkSession is already closed.");
        }
    }

}
