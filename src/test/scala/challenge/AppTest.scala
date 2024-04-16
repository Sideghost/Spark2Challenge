package challenge

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.types.{ArrayType, DateType, DoubleType, LongType, StringType}
import org.junit.{After, Before, Test}

class AppTest {

  private var spark: SparkSession = _

  @Before
  def setUp(): Unit = {
    spark = SparkUtils.getSparkSession
    spark.sparkContext.setLogLevel("ERROR")
  }


  @After
  def tearDown(): Unit = {
    if (spark != null)
      spark.stop()
  }

  @Test
  def `test part 1`(): Unit = {
    val df_1 = App.part1(spark)
    assert(df_1.columns.length == 2)

    //check if the Average_sentiment_polarity only has double values
    assert(df_1.schema.fields(1).dataType == DoubleType)
  }

  @Test
  def `test part 2`(): Unit = {
    val sparkSession = spark
    val df_2 = App.part2(sparkSession)
    import sparkSession.implicits._

    // ensuring that the dataframe has only 2 columns (App and Rating)
    assert(df_2.columns.length == 2)

    // check if the Rating only has double values
    assert(df_2.schema.fields(1).dataType == DoubleType)

    // check if the Rating column has no null or NaN values and all values are greater than 4.0
    assert(df_2.select($"Rating").filter($"Rating".isNull).count() == 0)
    assert(df_2.select($"Rating").filter($"Rating".isNaN).count() == 0)
    assert(df_2.select($"Rating").filter($"Rating" < 4.0).count() == 0)

    sparkSession.stop()
  }

  @Test
  def `test part 3`(): Unit = {
    val sparkSession = spark
    val df_3 = App.part3(sparkSession)
    import sparkSession.implicits._

    val expectedColumnNames = Array("App", "Categories", "Rating", "Reviews", "Size", "Installs", "Type", "Price",
      "Content_Rating", "Genres", "Last_Updated", "Current_Version", "Minimum_Android_Version")
    // ensuring the column names are correct
    assert(df_3.columns sameElements expectedColumnNames)

    val expectedTypes = Array(StringType, ArrayType(StringType, containsNull = false), DoubleType, LongType, DoubleType, StringType,
      StringType, DoubleType, StringType, ArrayType(StringType), DateType, StringType, StringType)

    // ensuring the column types are correct
    for (i <- expectedColumnNames.indices) {
      assert(df_3.schema.fields(i).dataType == expectedTypes(i))
    }

    // check if theres any Nan values in numeric columns
    for (col <- df_3.columns.filter(col => df_3.schema(col).dataType == DoubleType || df_3.schema(col).dataType == LongType)) {
      assert(df_3.select(col).filter(df_3(col).isNaN).count() == 0)
    }

    // checking if theres repeated apps
    val windowSpec = Window.partitionBy("App")
    assert(df_3.withColumn("row_number", row_number().over(windowSpec.orderBy($"Reviews"))).filter($"row_number" > 1).count() == 0)

    sparkSession.stop()
  }

  @Test
  def `test part 4`(): Unit = {
    val df_1 = App.part1(spark)
    val df_3 = App.part3(spark)
    val df_4 = App.part4(df_1, df_3)

    assert(df_4.columns.length == df_1.columns.length + df_3.columns.length - 1)
    assert(df_4.columns.contains("App"))
    assert(df_4.columns.contains("Genres"))
    assert(df_4.columns.contains("Average_Sentiment_Polarity"))
  }

  @Test
  def `test part 5`(): Unit = {
    val df_1 = App.part1(spark)
    val df_3 = App.part3(spark)
    val df_4 = App.part4(df_1, df_3)
    val df_5 = App.part5(spark, df_4)

    assert(df_5.columns.length == 4)
    assert(df_5.columns sameElements Array("Genre", "Count", "Average_Rating", "Average_Sentiment_Polarity"))
    assert(df_4.select("Genres").distinct().count() > df_5.select("Genre").distinct().count())
  }
}
