/**
  * Created by m.friaa on 26/07/2017.
  */
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
object App extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = new SparkUtil
  //Read Data from all the Parquet Files
  //val impDF = spark.sparkSession.read.parquet("C:\\events\\impressions\\*\\*")
  //Read Data from  a Parquet File to Test
  val rawImpDF = spark.sparkSession.read.parquet("C:\\events\\impressions\\hour=12\\*")
  // Get Data Schema
  //rawImpDF.printSchema()
  // Select needed Data to prepare it for the GroupBy
  val impDF = rawImpDF.select(
    rawImpDF("request_id"),
    rawImpDF("application_id"),
    rawImpDF("ad_format"),
    rawImpDF("user_country_code"),
    rawImpDF("integration"),
    rawImpDF("trackable"),
    rawImpDF("provider"),
    rawImpDF("application_user_id"),
    // If device_id is null or empty or equals "00000000-0000-0000-0000-000000000000" we should use application_user_id
    when((rawImpDF("device_id").isNull)
      || (rawImpDF("device_id") == "")
      || (rawImpDF("device_id") == "00000000-0000-0000-0000-000000000000"), rawImpDF("application_user_id"))
      // Otherwise we should use it
      .otherwise(rawImpDF("device_id")).as("device_id"),
    rawImpDF("publisher_revenue_source_type")
  // add filter to eliminate data containing false trackable
  ).filter(rawImpDF("trackable") === true)
    // add filter to eliminate null rows
    .filter(row => !row.anyNull)
  //impDF.printSchema()
  // Get impression count by all the required rows
  val eventsDF = impDF.groupBy(impDF("application_id"), impDF("user_country_code"), impDF("integration"), impDF("ad_format"), impDF("device_id"), impDF("provider")).count()
    .withColumn("placement_identifier", lit("not found"))
  // Get impression count with all provider values
  val eventsProviderDF = impDF.groupBy(impDF("application_id"), impDF("user_country_code"), impDF("integration"), impDF("ad_format"), impDF("device_id")).count()
    .withColumn("provider", lit(""))
   .withColumn("placement_identifier", lit("not found"))
  // Get impression count with all placement values
  //val eventsPlacementDF = impDF.groupBy(impDF("application_id"), impDF("user_country_code"), impDF("integration"), impDF("ad_format"), impDF("device_id"), impDF("provider")).count()
  // .withColumn("placement_identifier", lit("%ALL%"))
  // Get union of previous impression count Data Frames
  val eventsFinalDF = eventsDF.union(eventsProviderDF  )
  //.union(eventsPlacementDF)
  //println(impressionDF.count())
  //eventsDF.show(false)
  // Prepare the Final data to show
  val eventsDF2 = eventsFinalDF.select(
    "application_id",
    "user_country_code",
    "integration",
    "ad_format",
    "provider",
    "count",
    "placement_identifier"
  ).orderBy(desc("count"))
  // Show Final Data
  eventsDF2.show()
  // Insert Data into new File
  //eventsDF2.write.mode(SaveMode.Overwrite).csv("C:\\eventsResult\\impressionResults")
}
