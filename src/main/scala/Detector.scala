package main

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object Detector{
  def main(args: Array[String]) {
    val spark = SparkSession.builder
                            .master("local")
                            .appName("Fraud Detector")
                            .config("spark.driver.memory", "2g")
                            .enableHiveSupport()
                            .getOrCreate()
    import spark.implicits._
    val financesDF = spark.read.json("Data/finances-small.json").cache()
    financesDF
         .na.drop("all", Seq("ID", "Account", "Amount", "Description", "Date"))
         .na.fill("Unknown", Seq("Description"))
         .where($"Amount" =!= 0 || $"Description" === "Unknown")
         .selectExpr("Account.Number as AccountNumber", "Amount", "Date", "Description")
         .write.mode(SaveMode.Overwrite).parquet("Output/finances-small")
    
    // Output the records that are corrupt
    if (financesDF.hasColumn("_corrupt_record")) {
      financesDF.where($"_corrupt_record".isNotNull).select($"_corrupt_record")
                .write.mode(SaveMode.Overwrite).text("Output/corrupt-finances")
    }

    // Query to get account full name and account number
    financesDF
          .select(concat($"Account.FirstName", lit(" "), $"Account.LastName").as("FullName"),
                  $"Account.Number".as("AccountNumber"))
          .distinct
          .coalesce(5)
          .write.mode(SaveMode.Overwrite).json("Output/finances-small-accounts")
    
    // Query to get account details
    financesDF
          .select($"Account.Number".as("AccountNumber"), $"Amount", $"Description", $"Date")
          .groupBy($"AccountNumber")
          .agg(avg($"Amount").as("AverageTransaction"),
               sum($"Amount").as("TotalTransactions"),
               count($"Amount").as("NumberOfTransactions"),
               max($"Amount").as("MaxTransaction"),
               min($"Amount").as("MinTransaction"),
               stddev($"Amount").as("StandardDeviationAmount"),
               collect_set($"Description").as("UniqueTransactionDescriptions"))
          .coalesce(5)
          .write.mode(SaveMode.Overwrite).json("Output/finances-small-account-details")
  }

  // Implicit class to add the hasColumn method available to DataFrame
  implicit class DataFrameHelper(df: DataFrame) {
    import scala.util.Try
    def hasColumn(columnName: String) = Try(df(columnName)).isSuccess
  }
}