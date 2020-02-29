import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.{After, Before, Test}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame

/**
  * Created by sascha on 6/14/16.
  */
@Test
class IsbnEncoderTest {

  private var spark: SparkSession = null

  /**
    * Create Spark context before tests
    */
  @Before
  def setUp(): Unit = {
    spark = {
      SparkSession.builder().appName("IsbnEncoderTest").master("local").getOrCreate()
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
  def TestValidIsbn(): Unit = {
    val r1 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, "ISBN: 978-1555538624")

    val records = Seq(r1)
    val df = spark.createDataFrame(records)
    val df_r = df.select(df("Name"),df("Year"),explode(array(df("ISBN"))).alias("ISBN"))

    val IsbnDF = df_r.toDF()
    IsbnDF.createOrReplaceTempView("IsbnTable")
    val df3 = spark.sqlContext.sql("select ISBN from IsbnTable").collect().mkString

    val ISBNnmbr = df3.replaceAll("[-| ]", "")


    val ean = "ISBN-EAN: " + ISBNnmbr.slice(6,9)
    println(ean)
    val grp = "ISBN-GROUP: " + ISBNnmbr.slice(9,11)
    println(grp)
    val pub = "ISBN-PUBLISHER: " + ISBNnmbr.slice(11,15)
    println(pub)
    val tit = "ISBN-TITLE: " + ISBNnmbr.slice(15,18)
    println(tit)

    //val df4 = spark.sqlContext.sql("Insert into IsbnTable")
    val r2 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, ean)
    val r3 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, grp)
    val r4 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, pub)
    val r5 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, tit)

    val records1 = Seq(r2,r3,r4,r5)
    val df2 = spark.createDataFrame(records1)
    val ISBNData = Seq(df, df2)
    ISBNData.reduce(_ union _).show
    val OutputTable = ISBNData.reduce(_ union _).toDF()


    val isbnNumber = ISBNnmbr.slice(6,19)


    val newString = isbnNumber.replaceAll("[ |-]", "")

    val xs = newString.toList.map(x => x.toString.toInt)
    println("Check whether given ISBN Input string is a valid 13 digit Number or not?")
    val sum = (0 to xs.size - 1).map{ i => if (i % 2 == 0) xs(i) * 1 else xs(i) * 3}.foldLeft(0){(r, x) =>  r+x}
    if (sum % 10 == 0) println("The Given ISBN is a Valid ISBN") else  println("The given ISBN is an Invalid ISBN")
    //val tailNum = sum.toString.last.toString.toInt
    //val checkDgt = 10 - tailNum


    assert(5 == OutputTable.count())
    assert(5 == OutputTable.filter(col("name") === "Learning Spark: Lightning-Fast Big Data Analysis").count())
    assert(5 == OutputTable.filter(col("year") === 2015).count())

    assert(1 == OutputTable.filter(col("isbn") === "ISBN: 978-1555538624").count())

    assert(1 == OutputTable.filter(col("isbn") === "ISBN-EAN: 978").count())
    assert(1 == OutputTable.filter(col("isbn") === "ISBN-GROUP: 15").count())
    assert(1 == OutputTable.filter(col("isbn") === "ISBN-PUBLISHER: 5553").count())
    assert(1 == OutputTable.filter(col("isbn") === "ISBN-TITLE: 862").count())
  }

  @Test
  def TestInvalidIsbn(): Unit = {
    val r1 = Isbn("My book", 2014, "38543254-G")

    val records = Seq(r1)
    val df = spark.createDataFrame(records)

    val df_r = df.select(df("Name"),df("Year"),explode(array(df("ISBN"))).alias("ISBN"))
    df_r.show()

    assert(1 == df_r.count())
    assert(df_r.first().get(2) == "38543254-G")
  }

  @Test
  def TestEmptyIsbn(): Unit = {
    val r1 = Isbn("My book", 2014, "")

    val records = Seq(r1)
    val df = spark.createDataFrame(records)

    val df_r = df.select(df("Name"),df("Year"),explode(array(df("ISBN"))).alias("ISBN"))

    assert(1 == df_r.count())
    assert(df_r.first().get(2) == "")
  }

  @Test
  def TestMixed(): Unit = {
    val r1 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, "ISBN: 978-1449358624")
    val r2 = Isbn("Scala Tutorial", 2016, "38543254-G")

    val records = Seq(r1, r2)
    val df = spark.createDataFrame(records)
    val df_r =df.select(df("Name"),df("Year"),explode(array(df("ISBN"))).alias("ISBN"))
    df_r.show()

    assert(2 == df_r.count())
    assert(df_r.first().get(2) == "ISBN: 978-1449358624")

  }

  case class Isbn(name: String, year: Int, isbn: String)
}
