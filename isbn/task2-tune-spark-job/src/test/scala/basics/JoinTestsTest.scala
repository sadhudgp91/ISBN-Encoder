package basics

import com.bosch.measurement.Transformation._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.{After, Before, Test}
import org.scalatest.Assertions._

@Test
class JoinTestsTest {

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
  def TestBasics(): Unit = {
    val r1 = Record("x011", "test no.1", "test", "failed", 1.9)
    val r2 = Record("x011", "measurement xy", "measurement", null, 2.1)
    val r3 = Record("x011", "measurement xx", "measurement", null, 2.2)
    val r4 = Record("x012", "test no.1", "test", "passed", 1.4)
    val r5 = Record("x012", "measurement xy", "measurement", null, 2.5)
    val r6 = Record("x011", "measurement xx", "measurement", null, 2.2)
    val r7 = Record("x013", "measurement xx", "measurement", null, 2.3)

    val df = spark.createDataFrame(Seq(r1, r2, r3, r4, r5, r6, r7))
    val df_pt = df.prepareTests
    val df_pm = df.prepareMeasurements

    val df_r = df_pt.joinTests(df_pm).cache

    assertResult(2) {
      df_r.count
    }

    assertResult(2) {
      df_r.groupBy(col("part_id")).count.count
    }

    assertResult(1) {
      df_r.groupBy(col("test_name"), col("name")).count.count
    }

    assertResult("measurement xy") {
      df_r.select(col("name")).first.getString(0)
    }

    assertResult("test no.1") {
      df_r.select(col("test_name")).first.getString(0)
    }
  }
}
