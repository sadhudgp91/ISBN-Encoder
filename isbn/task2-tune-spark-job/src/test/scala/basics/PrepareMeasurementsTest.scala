package basics

import com.bosch.measurement.Transformation._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.{After, Before, Test}
import org.scalatest.Assertions._

@Test
class PrepareMeasurementsTest {

  private var spark: SparkSession = null

  /**
    * Create Spark context before tests
    */
  @Before
  def setUp(): Unit = {
    spark = {
      SparkSession.builder().appName(getClass.getSimpleName).master("local").getOrCreate()
    }
  }

  /**
    * Stop Spark context after tests
    */
  @After
  def tearDown(): Unit = {
    spark.stop()
    spark = null
  }

  @Test
  def TestRemoveMeasurementsWithoutVariance(): Unit = {
    val r1 = Record("x011", "measurement xy", "measurement", null, 1.9)
    val r2 = Record("x011", "measurement yy", "measurement", null, 2.0)
    val r3 = Record("x012", "measurement xy", "measurement", null, 1.4)
    val r4 = Record("x012", "measurement yy", "measurement", null, 2.0)
    val r5 = Record("x012", "test no.2", "test", "passed", 2.1)

    val df_r = spark.createDataFrame(Seq(r1, r2, r3, r4, r5)).prepareMeasurements.cache

    assertResult(2) {
      df_r.count
    }

    assertResult(2) {
      df_r.groupBy(col("part_id")).count.count
    }

    assertResult(2) {
      df_r.filter(col("name").endsWith("xy")).count
    }
  }

}
