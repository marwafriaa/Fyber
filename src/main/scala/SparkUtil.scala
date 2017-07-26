import org.apache.spark.sql.SparkSession

/**
  * Created by m.friaa on 26/07/2017.
  */

class SparkUtil{

  val sparkSession = SparkSession.builder.master("local[*]")
    .appName("Fyber Challenge")
    .getOrCreate()
  val sc=sparkSession.sparkContext

  val sqlContext= sparkSession.sqlContext

}