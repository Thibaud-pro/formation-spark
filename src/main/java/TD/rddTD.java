package TD;

import model.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class rddTD {

    public static void run(SparkSession spark) {
        //Lecture du fichier personnes.txt
        JavaRDD<String> lines = spark
                .read()
                .textFile("src/main/resources/rddTD/personnes.txt")
                .javaRDD();
        //sc.textFile("src/main/resources/personnes.txt");

        //Affichage du contenu du fichier
        lines.collect().forEach(System.out::println);
        lines.foreach(l -> System.out.println(l.toString()));
        //Affichage de la longueur de chaque ligne
        lines.map(String::length).collect().forEach(System.out::println);

        //Convertir le précédent RDD en RDD de Person
        JavaRDD<Person> personRDD = lines.map(line -> {
            String[] parts = line.split(",");
            model.Person person = new model.Person();
            person.setNAME(parts[0]);
            person.setAGE(Integer.parseInt(parts[1]));
            person.setCOUNTRY(parts[2]);
            return person;
        });

        //Afficher chaque personne et ses infos
        personRDD.foreach(p -> System.out.println("Nom: " + p.NAME + " a " + p.AGE + "ans"));
    }
}
