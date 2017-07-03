package SessionAnalysis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._

/**
  * Created by Administrator on 2017/7/3.
  *
  * Main entry of the whole project
  *
  */
object SessionAnalysis {
  val DEBUG = true
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Shakespear").set("spark.shuffle.consolidateFiles", "true")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val inputPath = { if(DEBUG) "C:\\a\\test\\user.txt" else args(0) }
    val outputPath = args(1)
    val numProcessors = { if(DEBUG) 4 else 4 }

    val userRDD = sc.textFile(inputPath, numProcessors)
    val schemaString = "user_id username name age profession city sex"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = userRDD.map(_.split(" "))
      .map(record => Row(record(0), record(1), record(2), record(3), record(4), record(5)))

  }
}
