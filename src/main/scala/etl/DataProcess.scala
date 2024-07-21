package etl
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataProcess {

  def processData(df: DataFrame): DataFrame = {

    val cleanedDF = df
      .withColumn("UnitPrice", when(col("UnitPrice").isNull, 0).otherwise(col("UnitPrice")))
      .withColumn("Quantity", when(col("Quantity").isNull, 0).otherwise(col("Quantity")))

    val statistics = cleanedDF.describe("UnitPrice", "Quantity")
    statistics.show()

    val productAggregations = cleanedDF.groupBy("StockCode")
      .agg(
        sum(col("UnitPrice") * col("Quantity")).as("Total_Sales"),
        sum(col("Quantity")).as("Total_Quantity"),
        avg(col("UnitPrice")).as("Average_Price")
      )

    val countryAggregations = cleanedDF.groupBy("Country")
      .agg(
        sum(col("UnitPrice") * col("Quantity")).as("Total_Sales_By_Country"),
        avg(col("Quantity")).as("Average_Quantity_By_Country")
      )

    val monthlySales = cleanedDF
      .withColumn("Month", month(col("InvoiceDate")))
      .groupBy("Month")
      .agg(
        sum(col("UnitPrice") * col("Quantity")).as("Total_Monthly_Sales"),
        avg(col("Quantity")).as("Average_Monthly_Quantity")
      )

    val transformedDF = cleanedDF
      .withColumn("Sales_Per_Quantity", col("UnitPrice"))


    transformedDF
  }
  def calculateMonthlySales(df: DataFrame): DataFrame = {
    val dfWithTimestamp = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "MM/dd/yyyy HH:mm"))

    val monthlySales = dfWithTimestamp
      .withColumn("Month", month(col("InvoiceDate")))
      .groupBy("Month")
      .agg(
        sum(col("UnitPrice") * col("Quantity")).as("Total_Monthly_Sales"),
        countDistinct(col("InvoiceNo")).as("Invoice_Count")
      )
      .withColumn("Average_Monthly_Revenue_Per_Invoice",
        col("Total_Monthly_Sales") / col("Invoice_Count")
      )
      .orderBy("Month")

    monthlySales.show()

    monthlySales
  }
}
