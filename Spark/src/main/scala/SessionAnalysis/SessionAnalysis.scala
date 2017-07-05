package SessionAnalysis

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/7/3.
  *
  * Main entry of the whole project
  *
  */
object SessionAnalysis {
  val DEBUG = true
  val PRINT_DEBUG_INFO = true
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Session Analysis").set("spark.shuffle.consolidateFiles", "true")
      .setMaster("local[1]")
    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)

    val inputPath = { if(DEBUG) "" else args(0) }
    val outputPath = { if(DEBUG) "" else args(1) }
    val datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val userFilterCond = new UserFilterCondition(Some(18), Some(50), None, None)
    val users = UserFilter.run(sc, inputPath, outputPath, userFilterCond)

    val sessionLeftBound = datetimeFormat.parse("2017-01-01 00:00:00")
    val sessionRightBound = datetimeFormat.parse("2017-07-01 00:00:00")
    val keywords = Set("冰箱", "iphone6", "电视", "葡萄干", "尿不湿",
      "耳机", "小米5", "蚊帐", "牛排", "U盘")
    val categories = (1 to 10).map(_.toString).toSet

    // get valid sessions
    var categoryStatAcc = RegisterCategoryAccumulators.run(sc, "")
    var sessionStatAcc = RegisterSessionAccumulators.run(sc)
    val sessionFilterCond = new SessionFilterCondition(
      Some(sessionLeftBound), Some(sessionRightBound), None, None)
    val sessionRecords = SessionFilter.run(
      sc, inputPath, outputPath, sessionFilterCond, users, categoryStatAcc, sessionStatAcc)
      .collect()

    // get top10 categories
    // 惰性求值把我坑惨了！
    val numCategoriesDemanded = 10
    val categoryStat = categoryStatAcc.value.map(stat =>
      (stat._1, (stat._2(categoryStatAcc.CLICK), stat._2(categoryStatAcc.ORDER), stat._2(categoryStatAcc.PAY))))
      .toList
      .sortBy(_._2).reverse.take(numCategoriesDemanded)

    val sessionStat = sessionStatAcc.sumUpStats()


    if(DEBUG && PRINT_DEBUG_INFO)
    {
//      users.foreach(user => println(user._1, user._2.reduce(_+" "+_)))
//      sessionRecords.foreach(rec => println(rec._1, rec._2._1.reduce(_+" "+_), " | ", rec._2._2.reduce(_+" "+_)))
      sessionRecords.foreach(println)
      println(users.count())
      println(sessionRecords.length)
      categoryStat.foreach(println)
      sessionStat.toList.sortBy(_._1).foreach(println)
    }

  }

}
