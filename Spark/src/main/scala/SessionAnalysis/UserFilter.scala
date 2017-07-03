package SessionAnalysis

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/7/3.
  */
object UserFilter {
  val DEBUG = true

  def run(sc: SparkContext, input: String, output: String, cond: UserFilterCondition): RDD[(String, Array[String])] =
  {
    //    val sqlContext = new SQLContext(sc)

    val inputPath = { if(DEBUG) "C:\\a\\test\\user.txt" else input }
    val outputPath = { if(DEBUG) "" else output }
    val numProcessors = { if(DEBUG) 4 else 4 }

    val condition = sc.broadcast(cond)

    val users = sc.textFile(inputPath, numProcessors)
      .map(line => line.split("\\s+"))
      .filter(condition.value isValidUser)
      .map(array => (array(0), Array(array(1), array(2), array(3), array(4), array(5), array(6))))
    return users

    //    val userRDD = sc.textFile(inputPath, numProcessors)

    /*//Silly. Naive.
        val schemaString = "user_id username name age profession city sex"
        val fields = schemaString.split(" ")
          .map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)

        val rowRDD = userRDD.map(_.split(" "))
          .map(record => Row(record(0), record(1), record(2), record(3), record(4), record(5)))
    */
  }

}
