package SessionAnalysis

import org.apache.spark.SparkContext

/**
  * Created by Administrator on 2017/7/4.
  */
object RegisterCategoryAccumulators {
  val DEBUG = true
  val PRINT_DEBUG_INFO = false
  def run(sc: SparkContext, DataPath: String): CategoryStatAccumulator =
  {
    val dataPath = {if(DEBUG) "C:\\a\\test\\category.txt" else DataPath}

//    var stat: Map[Int, Array[Int]] = Map[Int, Array[Int]]()
    var stat: Map[Int, Array[Int]] = sc.textFile(dataPath)
      .map(_.split("\t")(0).toInt -> Array(0, 0, 0)).collect().toMap

    if(DEBUG && PRINT_DEBUG_INFO)
    {
      println(stat(0)(2))
//      stat.foreach(println(_.))
    }


    val categoryStatAcc = new CategoryStatAccumulator(stat)
    sc.register(categoryStatAcc, "categoryStat")
    return categoryStatAcc
  }
}
