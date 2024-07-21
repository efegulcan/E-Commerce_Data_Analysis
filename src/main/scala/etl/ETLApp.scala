package etl

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

object ETLApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Ecommerce ETL Pipeline")
      .master("local[*]")
      .getOrCreate()

    val inputPath = "data/raw/ECommerceDataset.csv"
    val tempProcessedDataPath = "data/output/temp_processed_data"
    val finalProcessedDataPath = "data/output/ECommerceDataset.csv"
    val monthlySalesOutputPath = "data/output/Monthly_Sales_Report.csv"

    val rawData = DataReader.readData(spark, inputPath)
    val processedData = DataProcess.processData(rawData)
    val monthlySales = DataProcess.calculateMonthlySales(rawData)

    println("Monthly Sales Data:")
    monthlySales.show()

    processedData.coalesce(1).write
      .option("header", "true")
      .csv(tempProcessedDataPath)

    monthlySales.coalesce(1).write
      .option("header", "true")
      .csv("data/output/temp_monthly_sales")

    val hadoopConf = new Configuration()
    val fs = FileSystem.get(hadoopConf)

    def renameFile(tempDir: String, finalFile: String): Unit = {
      val tempPath = new Path(tempDir)
      val outputPath = new Path(finalFile)

      val fileStatus = fs.listStatus(tempPath)
      fileStatus.foreach { status =>
        if (status.getPath.getName.startsWith("part-")) {
          val oldFilePath = status.getPath
          fs.rename(oldFilePath, outputPath)
        }
      }
      fs.delete(tempPath, true)
    }

    renameFile(tempProcessedDataPath, finalProcessedDataPath)
    renameFile("data/output/temp_monthly_sales", monthlySalesOutputPath)

    spark.stop()
  }
}
