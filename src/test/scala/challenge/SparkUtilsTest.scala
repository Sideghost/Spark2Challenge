package challenge

import org.junit.Test

class SparkUtilsTest {

  @Test
  def `Spark session should be created`(): Unit = {
    val sparkSession = SparkUtils.getSparkSession
    assert(sparkSession != null)
    sparkSession.stop()
  }

  @Test
  def `convert size to MB`(): Unit = {
    assert(SparkUtils.sizeToMb("1B") == 9.5367431640625E-7)
    assert(SparkUtils.sizeToMb("1K") == 0.0009765625)
    assert(SparkUtils.sizeToMb("1MB") == 1.0)
    assert(SparkUtils.sizeToMb("1GB") == 1024.0)
    assert(SparkUtils.sizeToMb("1TB") == 1048576.0)
    assert(SparkUtils.sizeToMb("Varies with device") == 0.0)
  }
}
