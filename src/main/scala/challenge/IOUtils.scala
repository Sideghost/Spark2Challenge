package challenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File
import scala.io.Source
import java.io.PrintWriter

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
      // Temporary directory to store CSV parts
      val tempDir = "temp_csv"
      df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(tempDir)

      // Directory object for the temporary directory
      val directory = new File(tempDir)
      val partFile = new File(directory.listFiles().filter(_.getName.startsWith("part")).head.getAbsolutePath)
      val pw = new PrintWriter(new File(path))

      // Write contents of the part file to the final CSV file
      val src = Source.fromFile(partFile)
      src.getLines.foreach(pw.println)
      src.close()
      pw.close()

      // Clean up temporary files
      directory.listFiles().foreach(_.delete())
      directory.delete()
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
