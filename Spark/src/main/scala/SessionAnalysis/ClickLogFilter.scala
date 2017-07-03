package SessionAnalysis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2017/7/3.
  */
object ClickLogFilter {
  val DEBUG = true

  def run(sc: SparkContext, input: String, output: String, cond: UserFilterCondition): RDD[(String, Array[String])] = {
    val inputPath = {
      if (DEBUG) "C:\\a\\test\\user.txt" else input
    }
    val outputPath = {
      if (DEBUG) "" else output
    }
    val numProcessors = {
      if (DEBUG) 4 else 4
    }

    //    val condition = sc.broadcast(cond)

    val users = sc.textFile(inputPath, numProcessors)
      .map(line => line.split("\\s+"))
      .filter(cond isValidUser)
      .map(array => (array(0), Array(array(1), array(2), array(3), array(4), array(5), array(6))))
    return users
  }
}
