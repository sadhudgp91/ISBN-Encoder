package com.bosch.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object IsbnEncoder {
  implicit def dmcEncoder(df: DataFrame) = new IsbnEncoderImplicit(df)

  case class Isbn(Name:String, Year:Int, ISBN:String)

  def explodeIsbn() = {

    /** Spark Configurations & Set SparkContexts **/

    val conf = new SparkConf().setAppName("Tests").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Tests").master("local").getOrCreate()

    println("INPUT SEQUENCE TABLE")

    /** Input Table Data (the ISBN number is taken as a sample)**/

    val inputrow = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, "ISBN: 978-1449358624")


    /**  Create Dataframe df after reading the record and storing the sequence in r1 **/

    val records = Seq(inputrow)
    val df2 = spark.createDataFrame(records)
    df2.show()

    /**  Create Temporary Table IsbnTable to query data if needed **/

    val IsbnDF = df2.toDF()
    IsbnDF.createOrReplaceTempView("IsbnTable")
    val ISBNQuery = spark.sqlContext.sql("select ISBN from IsbnTable").collect().mkString
    val ISBNnmbr = ISBNQuery.replaceAll("[-| ]", "")

    /**  Seperate the four different parts of ISBN Code **/

    val ean = "ISBN-EAN:" + ISBNnmbr.slice(6,9)

    val grp = "ISBN-GROUP:" + ISBNnmbr.slice(9,11)

    val pub = "ISBN-PUBLISHER:" + ISBNnmbr.slice(11,15)

    val tit = "ISBN-TITLE:" + ISBNnmbr.slice(15,18)

    val isbnNumber = ISBNnmbr.slice(6,19)

    val numberList = isbnNumber.toList.map(x => x.toString.toInt)

    /**  Check for Valid 13 Digit ISBN Number **/


    println("Check whether given ISBN Input string is a valid 13 digit Number or not?")
    val sum = (0 to numberList.size - 1).map{ i => if (i % 2 == 0) numberList(i) * 1 else numberList(i) * 3}.foldLeft(0){(r, x) =>  r+x}
    if (sum % 10 == 0) println("The Given ISBN is a Valid ISBN") else  println("The given ISBN is an Invalid ISBN")

    // for explode function $ string symbol and toDF

    import spark.implicits._

    println("OUTPUT TABLE")

    /**  Pass the variables for 4 elements of ISBN code to Seq and then Parallelize
      * Then use the explode function to seperate each element of the ISBN values into new row **/


    val RDDSequence = sc.parallelize(Seq(("Learning Spark: Lightning-Fast Big Data Analysis", 2015, Seq("ISBN: 978-1449358624",ean,grp,pub,tit))))
    val ColumnNames = RDDSequence.toDF("name", "year","ISBN")

    val exploded = ColumnNames.withColumn("ISBN", explode($"ISBN"))
    val df = exploded.toDF()
    df.show()

    df
  }

  class IsbnEncoderImplicit(df: DataFrame) extends Serializable {

  }
  def main(args : Array[String]): Unit = {
    dmcEncoder(explodeIsbn)
  }


}
