package challenge

import challenge.IOUtils.readCSVToDF
import org.apache.spark.sql.SparkSession
import org.junit.{After, Before, Test}
import org.scalactic.TypeCheckedTripleEquals.convertToCheckingEqualizer
import org.scalatest.Matchers.unconstrainedEquality

class IOUtilsTest {

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
  def `read csv file to DF`(): Unit = {
    val df = readCSVToDF(spark, "src/test/resources/test.csv")
    assert(df.count() == 8)
    assert(df.columns.length == 5)
  }

  @Test
  def `write DF to CSV`(): Unit = {
    val sparkSession = spark
    import sparkSession.implicits._

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )

    val df = simpleData.toDF("employee_name", "department", "salary")
    IOUtils.writeDFToCSV(df, "src/test/resources/test_output.csv")
    val df2 = readCSVToDF(sparkSession, "src/test/resources/test_output.csv")
    assert(df2.head() === df.head())
    assert(df2.count() == df.count())
    assert(df2.columns.length == df.columns.length)
    assert(df.collect() === df2.collect())
    sparkSession.stop()
  }

  @Test
  def `write DF to Parquet with GZIP`(): Unit = {
    val sparkSession = spark
    import sparkSession.implicits._

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )

    val df = simpleData.toDF("employee_name", "department", "salary")
    IOUtils.writeParquetWithGZIP(df, "src/test/resources/test_output2.parquet")
    val df2 = sparkSession.read.parquet("src/test/resources/test_output2.parquet")
    assert(df2.head() === df.head())
    assert(df2.count() == df.count())
    assert(df2.columns.length == df.columns.length)
    assert(df.collect() === df2.collect())
    sparkSession.stop()
  }
}
