package etl

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataReader {

  def readData(spark: SparkSession, filePath: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
  }
}
