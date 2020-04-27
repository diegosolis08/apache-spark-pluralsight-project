package main

import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.scalalang.typed

case class Account(number: String, firstName: String, lastName: String)

case class Transaction(id: Long, account: Account, date: java.sql.Date, amount: Double, description: String)

case class TransactionForAverage(accountNumber: String, amount: Double, description: String, date: java.sql.Date)

object Detector{
  def main(args: Array[String]) {
    val spark = SparkSession.builder
                            .master("local")
                            .appName("Fraud Detector")
                            .config("spark.driver.memory", "2g")
                            .enableHiveSupport()
                            .getOrCreate()
    import spark.implicits._
    val financesDS = spark.read.json("Data/finances-small.json")
                          .withColumn("Date", to_date(unix_timestamp($"Date", "MM/dd/yyyy").cast("timestamp")))
                          .as[Transaction]
                          .cache()

    val accountNumberPrevious4WindowSpec = Window.partitionBy($"AccountNumber").orderBy($"Date").rowsBetween(-4, 0)
    val rollingAvgForPrevious4PerAccount = avg($"Amount").over(accountNumberPrevious4WindowSpec)

    financesDS
         .na.drop("all", Seq("ID", "Account", "Amount", "Description", "Date"))
         .na.fill("Unknown", Seq("Description")).as[Transaction]
        //  .filter(tx => (tx.amount != 0 || tx.description == "Unknown")) // This is the DS way to express the where, but it is not optimized by the Catalyst
         .where($"Amount" =!= 0 || $"Description" === "Unknown")
         .select($"Account.Number".as("AccountNumber").as[String], 
                 $"Amount".as[Double], 
                 $"Date".as[java.sql.Date](Encoders.DATE), 
                 $"Description".as[String])
         .withColumn("RollingAverage", rollingAvgForPrevious4PerAccount)
         .write.mode(SaveMode.Overwrite).parquet("Output/finances-small")
    
    // Output the records that are corrupt
    if (financesDS.hasColumn("_corrupt_record")) {
      financesDS.where($"_corrupt_record".isNotNull).select($"_corrupt_record")
                .write.mode(SaveMode.Overwrite).text("Output/corrupt-finances")
    }

    // Query to get account full name and account number
    financesDS          
          .map(tx => (s"${tx.account.firstName} ${tx.account.lastName}", tx.account.number))
          .distinct
          .toDF("FullName", "AccountNumber")
          .coalesce(5)
          .write.mode(SaveMode.Overwrite).json("Output/finances-small-accounts")
    
    // Query to get account details
    financesDS
          .select($"Account.Number".as("AccountNumber").as[String], 
                  $"Amount".as[Double], 
                  $"Description".as[String],
                  $"Date".as[java.sql.Date](Encoders.DATE)).as[TransactionForAverage]
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
  implicit class DataSetHelper[T](ds: Dataset[T]) {
    import scala.util.Try
    def hasColumn(columnName: String) = Try(ds(columnName)).isSuccess
  }
}