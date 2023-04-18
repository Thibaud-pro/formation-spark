package TP;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class rddTP {
    public static void wordCount(String fileName, SparkSession spark) throws IOException {
        //Ecriture du résultat dans un dossier
        final String outputPath = "src/main/resources/count";
        //Lecture des données d'entrée
        JavaRDD<String> lines = spark.read().textFile(fileName).javaRDD();
        //Découpage des mots selon l'espace
        //Pour pouvoir utiliser cette liste avec la méthode flatMap() de Spark, il faut la convertir en une collection d'éléments individuels.
        //Il faut appeler .iterator pour convertir le tableau en une instance de Iterator, qui est utilisée pour itérer sur les
        //éléments individuels de la liste.
        //L'utilisation d'un Iterator plutôt que d'une liste dans flatMap() permet également de réduire la consommation de mémoire,
        // car la liste complète de mots n'a pas besoin d'être stockée en mémoire avant le traitement.
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        //NB: map et flatMap sont deux opérations de transformation d'un RDD (Resilient Distributed Dataset) en un autre RDD.
        //map() est utilisé pour appliquer une transformation à chaque élément d'un RDD et renvoyer un nouveau RDD avec les éléments transformés
        //flatMap() est utilisé pour appliquer une transformation à chaque élément d'un RDD qui renvoie zéro, un ou plusieurs éléments et renvoie un nouveau RDD
        //avec tous les éléments renvoyés. Par exemple, si un RDD de lignes de texte et que l'on veut btenir tous les mots uniques dans toutes les lignes,
        //flatMap() pour diviser chaque ligne en mots et renvoyer une liste d'éléments

        //Appplication du word count
        JavaPairRDD<String, Integer> counts = words
                .mapToPair(t -> new Tuple2<>(t, 1))
                .reduceByKey(Integer::sum) // autre méthode ((a,b) -> a + b)
                .filter(e -> e._2 > 5)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap);

        //Affichage des mots présents plus de 5 fois
        counts.collect().forEach(t -> System.out.println(t._1 + " : " + t._2));

        //Ecriture de la sortie
        FileUtils.deleteDirectory(new File(outputPath));
        counts.saveAsTextFile(outputPath);
    }
}
