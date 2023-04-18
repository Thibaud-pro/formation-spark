package TD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class dataframeTD {

    public static void run(SparkSession spark) {
        // Chargement d’un fichier CSV en Dataframe
        Dataset<Row> df = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("sep", ";")
                .load("src/main/resources/dataframeTD/donnees_communes.csv");

        // Affichage des colonnes du Dataframe
        df.printSchema();

        // Afficher les 10 première lignées du Dataframe
        df.show(10);

        // Création d'une vue temporaire à partir du DataFrame
        df.createOrReplaceTempView("commune");

        // Réalisation d'une requête SQL sur la vue temporaire pour sélectionner les lignes où la valeur d'une colonne est supérieure à 50 ou un autre filtre
        Dataset<Row> results = spark.sql("SELECT * FROM commune WHERE CODCOM = '001'");

        // Affichage des résultats de la requête
        results.show();
    }
}
