package TP;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;

public class dataframeTP {
    public static String[] getColsMatchingRegexp(Dataset<Row> df, String regexp) {
        return Arrays.stream(df.columns()).filter(c -> c.matches(regexp)).toArray(String[]::new);
    }

    public static Dataset<Row> unPivotYearPopulationDf(String[] colsToUnPivot, Dataset<Row> df) {
        String selectExpr = "stack(" + (colsToUnPivot.length);
        for (String col : colsToUnPivot) {
            selectExpr += ", '" + col.replace("year_", "") + "', " + col;
        }
        selectExpr += ") as (Year, Population) ";

        return df.selectExpr("`Country Name`", selectExpr);
    }

    public static void run(SparkSession spark) {
        // Chargement d’un fichier CSV en Dataframe
        Dataset<Row> df = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("sep", ",")
                .load("src/main/resources/dataframeTP/population_mondiale.csv");

        String regexMatchingYear = "[0-9]{4}";
        String[] colsToBeRenamed = getColsMatchingRegexp(df, regexMatchingYear);

        for (String col : colsToBeRenamed) {
            df = df.withColumnRenamed(col, "year_" + col);
        }

        String regexMatchingYear_ = "year_.*";
        String[] colsToBuildExpr = getColsMatchingRegexp(df, regexMatchingYear_);

        Dataset<Row> unPivotPopulation = unPivotYearPopulationDf(colsToBuildExpr, df);
        System.out.println(unPivotPopulation.count());
        unPivotPopulation.show();

        // Définition d'une fenêtre pour grouper les données par pays
        WindowSpec window = Window.partitionBy("Country name").orderBy("year");

        Dataset<Row> DFCompareYear = unPivotPopulation
                .withColumn("population_previous_year", functions.lag("population", 1).over(window))
                .withColumn("population_change", functions.col("Population").minus(functions.col("population_previous_year")))
                .withColumn("change_percent", functions.col("population_change").divide(functions.col("population")).multiply(100))
                .withColumn("Year", functions.col("Year").cast(IntegerType))
                .withColumn("Population", functions.col("Population").cast(LongType))
                .na()
                .fill(0.0);

        DFCompareYear.show();

        // Convert the year column to a feature vector.
        //indique que la colonne nommée features contient les variables d'entrée (ou les caractéristiques) qui seront utilisées pour prédire la variable de sortie.
        //indique que la colonne nommée outputCol contient la variable de sortie (ou la cible) qui sera prédite en fonction des variables d'entrée.
        VectorAssembler assembler = new VectorAssembler().setInputCols(new String[]{"Year"}).setOutputCol("features");
        Dataset<Row> transformedData = assembler.transform(DFCompareYear).select("Country Name", "features", "Population");

        // Define the linear regression model
        //La fonction setMaxIter() de la bibliothèque MLlib de Spark permet de définir le nombre maximum d'itérations pour l'algorithme d'apprentissage.
        // Plus précisément, cette fonction définit le nombre maximal de fois que l'algorithme d'optimisation sera exécuté pour ajuster les paramètres du modèle.
        LinearRegression lr = new LinearRegression().setMaxIter(10).setLabelCol("Population").setFeaturesCol("features");

        // Fit the model
        LinearRegressionModel lrModel = lr.fit(transformedData);

        // Entraîner le modèle pour chaque pays
        List<String> countryList = df.select("Country Name").as(Encoders.STRING()).collectAsList().stream().filter(Objects::nonNull).collect(Collectors.toList());

        String[] countries = countryList.subList(0, 10).toArray(new String[0]);
        Map<String, LinearRegressionModel> models = Arrays.stream(countries)
                .collect(Collectors.toMap(
                        Function.identity(),
                        country -> {
                            Dataset<Row> countryData = transformedData.filter(functions.col("Country Name").equalTo(country));
                            return lr.fit(countryData);
                        }
                ));

        // Afficher les coefficients pour chaque pays
        models.forEach((country, model) -> System.out.println(country + ": y = " + model.coefficients().toArray()[0] + "x + " + model.intercept()));

        // Print the coefficients and intercept for linear regression
        // y = intercept + coefficient_x * x
        System.out.println("Coefficients: " + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

        // Print the summary of the model
        //La RMSE mesure la différence entre les valeurs prédites par le modèle et les valeurs réelles de la variable cible dans l'ensemble de données.
        // Elle calcule la moyenne des carrés de ces différences, puis en prend la racine carrée. Cela donne une mesure de l'écart moyen entre les prévisions du modèle et les observations réelles.
        //Un modèle avec une RMSE plus faible est considéré comme plus précis dans ses prévisions que celui avec une RMSE plus élevée: A tempérer
        LinearRegressionTrainingSummary summary = lrModel.summary();
        System.out.println("RMSE: " + summary.rootMeanSquaredError());


    }
}
