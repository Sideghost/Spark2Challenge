package challenge

import challenge.IOUtils.{readCSVToDF, writeDFToCSV, writeParquetWithGZIP}
import challenge.SparkUtils.{getSparkSession, sizeToMb}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DoubleType, LongType, StringType}

/**
 * Entry point
 */
object App {

  /**
   * Main method to run the application.
   *
   * @param args the command-line arguments
   */
  def main(args: Array[String]): Unit = {
    // shared spark session
    val sparkSession = getSparkSession

    // Set log level to ERROR so theres no log pollution
    sparkSession.sparkContext.setLogLevel("ERROR")

    // challenge steps
    val df_1 = part1(sparkSession)
    part2(sparkSession)
    val df_3 = part3(sparkSession)
    val df_4 = part4(df_1, df_3)
    part5(sparkSession, df_4)

    // Stop the SparkSession
    sparkSession.stop()
  }

  /**
   * Part 1: Calculate the average sentiment polarity for each app.
   * Steps:
   * 1. Load the data from the CSV file.
   * 2. Select the necessary columns and handle missing values.
   * 3. Group by 'App' and calculate the average of 'Sentiment_Polarity'.
   * 4. Optionally, save the DataFrame to a file.
   *
   * @param spark the SparkSession
   * @return
   */
  private def part1(spark: SparkSession): DataFrame = {
    // For implicit conversions like converting RDDs to DataFrames
    // how this works no idea
    import spark.implicits._

    // Load data from a CSV file
    val df = readCSVToDF(spark, "src/main/resources/googleplaystore_user_reviews.csv")

    // Select necessary columns and handle missing values
    val df_clean = df.select($"App", $"Sentiment_Polarity".cast("double"))
      .na.fill(0.0, Seq("Sentiment_Polarity"))  // Fill null sentiment polarity with 0.0

    // Group by 'App' and calculate the average of 'Sentiment_Polarity'
    val df_1 = df_clean.groupBy($"App")
      .agg(avg($"Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
      .na.fill(0.0, Seq("Average_Sentiment_Polarity"))  // Fill NaN results with 0.0

    // saving the df_1 as a file
    writeDFToCSV(df_1, "src/main/resources/output_df1")

    df_1
  }

  /**
   * Part 2: Find all apps with a rating greater or equal to 4.0 and sorting in descending order.
   * Steps:
   * 1. Load the data from the CSV file.
   * 2. Select the necessary columns and handle missing values.
   * 3. Filter the DataFrame to include only apps with a rating greater or equal to 4.0.
   * 4. Sort the resulting DataFrame in descending order.
   * 5. Optionally, save the DataFrame to a file.
   *
   * @param spark the SparkSession
   */
  private def part2(spark: SparkSession): Unit = {
    import spark.implicits._

    val df = readCSVToDF(spark, "src/main/resources/googleplaystore.csv")

    // all Apps with a "Rating" greater or equal to 4.0 sorted in descending order.
    val df_2 = df.select($"App", $"Rating".cast(DoubleType))
      .na.fill(0.0, Seq("Rating"))  // Fill null ratings with 0.0
      .filter($"Rating" >= 4.0)
      .sort($"Rating".desc)

    // saving the df_2 as a file
    writeDFToCSV(df_2, "src/main/resources/best_apps")
  }

  /**
   * Part 3: Process the Google Play Store data.
   * Steps:
   * 1. Load the data from the CSV file.
   * 2. Process the DataFrame to their correct types and formats.
   * 3. Collect the categories of the apps with the same name in
   * an array inside the column "Category" and drop duplicate categories.
   * 4. Sort by app and drop duplicates by number of reviews.
   * 5. Rename columns.
   * 6. Convert arrays to strings for saving.
   * 7. Optionally, save the DataFrame to a file.
   * TODO has a bug cause it doesnt properly delete duplicate apps
   * @param spark the SparkSession
   * @return the processed DataFrame
   */
  private def part3(spark: SparkSession): DataFrame = {
    // For implicit conversions like converting RDDs to DataFrames
    // how this works no idea
    import spark.implicits._

    val df = readCSVToDF(spark, "src/main/resources/googleplaystore.csv")

    // mapper to convert size to MB
    val sizeToMbUDF = udf(sizeToMb _)

    // Process the DataFrame to their correct types and formats
    val processedDf = df
      .withColumn("Category", $"Category".cast(StringType))
      .withColumn("Rating", $"Rating".cast(DoubleType))
      .withColumn("Reviews", $"Reviews".cast(LongType)).na.fill(0L, Seq("Reviews")) // Fill null reviews with 0
      .withColumn("Size", sizeToMbUDF($"Size")) // Convert size to MB
      .withColumn("Price", regexp_replace($"Price", "\\$", "").cast(DoubleType) * 0.9) // Convert price to EUR
      .withColumn("Last Updated", to_date($"Last Updated", "MMMM dd, yyyy")) // Convert date to DateType
      .withColumn("Genres", split($"Genres", ";").cast(ArrayType(StringType)))

    // collect the categories of the apps with the same name in an array inside the column "Category" and drop duplicate categories
    val windowSpec = Window.partitionBy("App")
    val df_3 = processedDf.withColumn("Category", collect_set($"Category").over(windowSpec))

    // sort by app and drop duplicates by nr of reviews
    val df_3_cleaned = df_3
      .withColumn("row_number", row_number().over(windowSpec.orderBy($"Reviews".desc)))
      .filter($"row_number" === 1)
      .drop("row_number")

    // rename columns
    val df_3_renamed = df_3_cleaned
      .withColumnRenamed("Category", "Categories")
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumnRenamed("Last Updated", "Last_Updated")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")

    // convert arrays to strings for saving
    val df_3_string = df_3_renamed
      .withColumn("Categories", $"Categories".cast(StringType))
      .withColumn("Genres", $"Genres".cast(StringType))

    writeDFToCSV(df_3_string, "src/main/resources/output_df3")

    df_3_renamed
  }

  /**
   * Part 4: Combine the DataFrames from Part 1 and Part 3.
   * Steps:
   * 1. Load the DataFrames from the Parquet files.
   * 2. Join the DataFrames on the "App" column.
   * 3. Write the result to a Parquet file with gzip compression.
   *
   * @param df_1 the DataFrame from Part 1
   * @param df_2 the DataFrame from Part 3
   * @return the combined DataFrame
   */
  private def part4(df_1: DataFrame, df_2: DataFrame): DataFrame = {
    // Join the DataFrames on the "App" column
    val combinedDf = df_2.join(df_1, Seq("App"), "left")

    // Write the result to a Parquet file with gzip compression
    writeParquetWithGZIP(df = combinedDf, path = "src/main/resources/googleplaystore_cleaned")

    combinedDf
  }

  /**
   * Part 5: Calculate metrics for each genre.
   * Steps:
   * 1. Create a column for each genre associated with an app.
   * 2. Group by 'Genre' and calculate metrics.
   * 3. Write the result to a Parquet file with gzip compression.
   *
   * @param spark the SparkSession
   * @param df the DataFrame from Part 4
   */
  private def part5(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._

    // Explode the 'Genres' array to create a row for each genre associated with an app
    val explodedGenresDf = df.withColumn("Genre", explode($"Genres"))

    // Group by 'Genre' and calculate metrics
    val genreMetricsDf = explodedGenresDf.na.fill(0L, Seq("Rating")).groupBy($"Genre")
      .agg(
        count($"App").alias("Count"),
        avg($"Rating").alias("Average_Rating"),
        avg($"Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )

    // Write the result to a Parquet file with gzip compression
    writeParquetWithGZIP(df = genreMetricsDf, path = "src/main/resources/googleplaystore_metrics")
  }
}