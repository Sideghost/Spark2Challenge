package challenge

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Utility functions for reading and writing data
 */
object IOUtils {
  /**
   * Read a CSV file into a DataFrame
   *
   * @param spark the SparkSession
   * @param path the path to the CSV file
   * @return the DataFrame
   */
  def readCSVToDF(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

    /**
     * Write a DataFrame to a CSV file
     *
     * @param df the DataFrame
     * @param path the path to the CSV file
     */
  def writeDFToCSV(df: DataFrame, path: String): Unit = {
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(path)
  }

  /**
   * Write a DataFrame to a Parquet file with GZIP compression
   *
   * @param df DataFrame to write
   * @param path Path to write the DataFrame
   */
  def writeParquetWithGZIP(df: DataFrame, path: String): Unit = {
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("compression", "gzip")
      .parquet(path)
  }
}
