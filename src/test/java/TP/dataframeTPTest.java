package TP;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utils.DataframeComparator;

import java.util.ArrayList;
import java.util.List;

public class dataframeTPTest {

    private SparkSession sparkSession;

    @BeforeEach
    public void setup() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Test App")
                .setMaster("local[*]");

        sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        sparkSession.sparkContext().setLogLevel("ERROR");
    }

    @Test
    public void testGetColsMatchingRegexp() {

        StructType structType = new StructType();
        structType = structType.add("A", DataTypes.StringType, false);
        structType = structType.add("b", DataTypes.StringType, false);

        List<Row> nums = new ArrayList<>();
        nums.add(RowFactory.create("value1", "value2"));

        Dataset<Row> df = sparkSession.createDataFrame(nums, structType);
        String regex = "[A-Z]";
        String regexBis = "1";

        String[] colsActual = dataframeTP.getColsMatchingRegexp(df, regex);
        String[] colsExpected = new String[]{"A"};

        String[] colsActualBis = dataframeTP.getColsMatchingRegexp(df, regexBis);
        String[] colsExpectedBis = new String[]{};

        Assertions.assertArrayEquals(colsExpected, colsActual);
        Assertions.assertArrayEquals(colsExpectedBis, colsActualBis);
    }

    @Test
    public void testUnPivotYearPopulationDf() {
        StructType inputStructType = new StructType();
        inputStructType = inputStructType.add("Country Name", DataTypes.StringType, false);
        inputStructType = inputStructType.add("year_2020", DataTypes.DoubleType, false);
        inputStructType = inputStructType.add("year_2021", DataTypes.DoubleType, false);
        inputStructType = inputStructType.add("year_2022", DataTypes.DoubleType, false);

        StructType expectedStructType = new StructType();
        expectedStructType = expectedStructType.add("Country Name", DataTypes.StringType, false);
        expectedStructType = expectedStructType.add("Year", DataTypes.StringType, false);
        expectedStructType = expectedStructType.add("Population", DataTypes.DoubleType, false);

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("France", 10.0, 20.0, 30.0));

        List<Row> expectedRows = new ArrayList<>();
        expectedRows.add(RowFactory.create("France", "2020", 10.0));
        expectedRows.add(RowFactory.create("France", "2021", 20.0));
        expectedRows.add(RowFactory.create("France", "2022", 30.0));

        String[] cols = new String[]{"year_2020","year_2021","year_2022"};

        Dataset<Row> expectedDf = sparkSession.createDataFrame(expectedRows, expectedStructType);

        Dataset<Row> df = dataframeTP.unPivotYearPopulationDf(cols, sparkSession.createDataFrame(rows, inputStructType));

        Assertions.assertTrue(DataframeComparator.compareDataframes(expectedDf, df, false));

    }

    @AfterEach
    public void tearDown() {
        // Make sure the SparkSession is closed after each test
        if (sparkSession != null) {
            sparkSession.close();
        }
    }
}
