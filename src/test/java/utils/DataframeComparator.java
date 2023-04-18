package utils;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataframeComparator {

    /**
     * Cette fonction prend deux Dataset<Row> en entrée, extrait leur schéma respectif en utilisant la méthode schema(),
     * et les compare champ par champ en utilisant les propriétés suivantes de chaque champ :
     *
     * name(): pour vérifier si les noms des champs sont identiques
     * dataType(): pour vérifier si les types de données des champs sont identiques
     * nullable(): pour vérifier si les champs ont la même propriété nullable
     * metadata(): pour vérifier si les champs ont les mêmes métadonnées
     *
     * @param df1
     * @param df2
     * @return
     */
    public static boolean compareDataframes(Dataset<Row> df1, Dataset<Row> df2, Boolean compareNullable) {

        StructType schema1 = df1.sort().schema();
        StructType schema2 = df2.sort().schema();
        int numFields1 = schema1.fields().length;
        int numFields2 = schema2.fields().length;

        if (numFields1 != numFields2) {
            System.out.println("Nombre de champs différents");
            return false;
        }

        for (int i = 0; i < numFields1; i++) {
            StructField field1 = schema1.fields()[i];
            StructField field2 = schema2.fields()[i];

            if (!field1.name().equals(field2.name())) {
                System.out.println("Nom des colonnes différents");
                return false;
            }
            if (!field1.dataType().equals(field2.dataType())) {
                System.out.println("Data type des colonnes différents");
                return false;
            }
            if (compareNullable && field1.nullable() != field2.nullable()) {
                System.out.println("Nullable des colonnes différents");
                return false;
            }
            if (!field1.metadata().equals(field2.metadata())) {
                System.out.println("Metadata des colonnes différents");
                return false;
            }
        }

        // Vérifier si les deux dataframes ont le même nombre de lignes
        if (df1.count() != df2.count()) {
            System.out.println("Nombre de lignes différentes");
            return false;
        }

        // Vérifier si les deux dataframes ont les mêmes données
        Dataset<Row> diff = df1.except(df2).union(df2.except(df1));
        if (diff.count() != 0) {
            System.out.println("Valeurs différentes");
            return false;
        }

        return true;
    }
}