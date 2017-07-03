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
    val conf = new SparkConf().setAppName("Session Analysis").set("spark.shuffle.consolidateFiles", "true")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)

    val inputPath = { if(DEBUG) "" else args(0) }
    val outputPath = { if(DEBUG) "" else args(1) }

    val cond = new UserFilterCondition(Some(18), Some(50), None, Some(Set("city26")))
    val users = UserFilter.run(sc, inputPath, outputPath, cond)

    users.foreach(user => println(user._1, user._2.reduce(_+" "+_)))
    println(users.count())
  }

}
