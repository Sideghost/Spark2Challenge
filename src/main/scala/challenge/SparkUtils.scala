package challenge

import org.apache.spark.sql.SparkSession

object SparkUtils {
  /**
   * Create a Spark session
   * @return
   */
  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .appName("Google Play Store User Reviews Analysis")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .master("local[*]")
      .getOrCreate()
  }

  /**
   * Converts sizes given in B, K, MB, GB, TB to megabytes.
   *
   * @param sizeStr The size string from the DataFrame.
   * @return The size in megabytes as a Double or None if the input is invalid.
   */
  def sizeToMb(sizeStr: String): Double = {
    val normalizedSize = sizeStr.toUpperCase.replace(" ", "")

    normalizedSize match {
      case s if s.endsWith("B") => s.replace("B", "").toDouble / 1024 / 1024
      case s if s.endsWith("K") => s.replace("K", "").toDouble / 1024
      case s if s.endsWith("MB") => s.replace("MB", "").toDouble
      case s if s.endsWith("GB") => s.replace("GB", "").toDouble * 1024
      case s if s.endsWith("TB") => s.replace("TB", "").toDouble * 1024 * 1024
      case _ => 0.0 // Handle "Varies with device" or any other malformed input
    }
  }
}
