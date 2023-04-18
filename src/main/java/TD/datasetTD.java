package TD;

import model.Person;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

public class datasetTD {

    public static void run(SparkSession spark) {

        // Création d'un Dataset à partir d'une liste d'objets Personne
        // Définition de l'encodage pour la classe Person
        Encoder<Person> personEncoder = Encoders.bean(Person.class);

        List<Person> personList = Arrays.asList(
                new Person("John", 15, "FRA"),
                new Person("Mike", 30, "USA"),
                new Person("Sara", 35, "ITA")
        );
        Dataset<Person> peopleDS = spark.createDataset(personList, personEncoder);

        // Affichage des colonnes du Dataset
        peopleDS.printSchema();

        // Affichage des 10 premières lignes du Dataset
        peopleDS.show(10);

        // Transformation du Dataset en augmentant l'âge de toutes les personnes de 10 ans
        Dataset<Person> olderPeopleDS = peopleDS.map(
                (MapFunction<Person, Person>) person -> new Person(person.getNAME(), person.getAGE() + 10, person.getCOUNTRY()),
                personEncoder
        );

        // Affichage des résultats de la transformation
        olderPeopleDS.show();

        //Ajouter une colonne au Dataset pour vérifier si une personne est adulte (age >= 18 ans), de 2 manières différentes: .withColumn et UDF
        peopleDS.withColumn("isAdult", peopleDS.col("age").gt(18)).show();

        spark.udf().register("isAdultUDF", (Integer age) -> age > 18, DataTypes.BooleanType);
        peopleDS.withColumn("isAdult", functions.callUDF("isAdultUDF", peopleDS.col("age"))).show();

    }

}
