package com.bosch.measurement

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

object Transformation {
  implicit def transformDf(df: DataFrame) = new TransformationImplicit(df)
}

class TransformationImplicit(df: DataFrame) extends Serializable {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def prepareTests(): DataFrame = {
    // filter for tests
    val df_filter_tests = df.filter(col("field_type") === "test")

    // remove tests without failures
    val df_keep = df_filter_tests.filter(col("result") === "failed").select(col("name")).dropDuplicates
    df_filter_tests.join(broadcast(df_keep), Seq("name"))
      .withColumnRenamed("name", "test_name")
      .drop("value", "field_type")
  }

  def prepareMeasurements(): DataFrame = {
    // filter for measurements
    val df_filter_measurements = df.filter(col("field_type") === "measurement")

    // remove measurements without variances
    val df_keep = df_filter_measurements.groupBy(col("name"))
      .agg(countDistinct(col("value")).as("count"))
      .filter(col("count") > 1)
      .select(col("name"))
      .dropDuplicates
    df_filter_measurements.join(broadcast(df_keep), Seq("name"))
      .drop("result", "field_type")
  }

  def joinTests(measurements: DataFrame): DataFrame = {
    // Join tests with measurements
    val df_joined = df.join(measurements, Seq("part_id"))

    // log the count of the data frame
    logger.info(df_joined.count + " elements found!")

    // double-check that we don't have any measurements without variance
    val df_keep = df_joined.groupBy(col("name"))
      .agg(countDistinct(col("value")).as("count"))
      .filter(col("count") > 1)
      .select(col("name"))
      .dropDuplicates
    df_joined.join(broadcast(df_keep), Seq("name"))
  }

}

