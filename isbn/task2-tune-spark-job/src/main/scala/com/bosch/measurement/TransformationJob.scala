package com.bosch.measurement

import java.io.File

import com.bosch.measurement.Transformation._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

object TransformationJob {
  val logger: Logger = LoggerFactory.getLogger(getClass)
  private val AppName = getClass.getSimpleName

  def main(args: Array[String]) {
    // Check arguments
    if (args.length < 3) {
      System.err.println(s"Usage: ${AppName} <Configuration> <InputPath> <OutputPath>");
      System.exit(1);
    }

    // load application configuration
    val fileConfig = ConfigFactory.parseFile(new File(args(0)))
    val config = ConfigFactory.load(fileConfig)

    // get the Spark context object
    val spark = SparkSession.builder().appName(AppName).getOrCreate()
    val df = spark.read.parquet(args(1))

    // prepare measurements and tests
    val df_tests = df.prepareTests
    val df_measurements = df.prepareMeasurements

    // write joined table back
    df_tests.joinTests(df_measurements)
      .write.mode(SaveMode.Overwrite).parquet(args(2))

    // clean up
    spark.stop()
  }
}


