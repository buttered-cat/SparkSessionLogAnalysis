package SessionAnalysis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import PublicVals._

/**
  * Created by Administrator on 2017/7/3.
  *
  * Indexes: (better off using Map...but I'm lazy and it's important to be idle :)
  *
  * (userId1, (sessionId2, date0, pageId3, actionTime4 + 5, keywords6, clickCategoryId7
  * , clickProductId8, orderCategoryId9, orderProductId10, payCategoryId11
  * , payProductId12, randomInt13))
  *
  * @return
  *         RDD[(userID, ((Array of user info), (Array of session record info)))]
  *
  */
object SessionFilter {
  val DEBUG = true

  def run(sc: SparkContext, input: String, output: String
          , cond: SessionFilterCondition
          , users: RDD[(String, Array[String])]): RDD[(String, (Array[String], Array[String]))] =
  {
    val inputPath = {
      if (DEBUG) "C:\\a\\test\\click.log" else input
    }
    val outputPath = {
      if (DEBUG) "" else output
    }
    val numProcessors = {
      if (DEBUG) 4 else 4
    }

    val condition = sc.broadcast(cond)

    val clickRecords = sc.textFile(inputPath, numProcessors)
      .map(line => line.split("\t"))
      .filter(condition.value isValidClick)
      .map(array => (array(sessionRawIndex("userID")),
        Array(array(sessionRawIndex("sessionID"))
          , array(sessionRawIndex("sessionActionID"))
          , array(sessionRawIndex("date"))
          , array(sessionRawIndex("pageID"))
          , array(sessionRawIndex("actionTime"))
          , array(sessionRawIndex("keywords"))
          , array(sessionRawIndex("clickCategoryID"))
          , array(sessionRawIndex("clickProductID"))
          , array(sessionRawIndex("orderCategoryID"))
          , array(sessionRawIndex("orderProductID"))
          , array(sessionRawIndex("payCategoryID"))
          , array(sessionRawIndex("payProductID"))
          , array(sessionRawIndex("reservedField"))
        )))


    val clickRecordsOfUsers = users.join(clickRecords)

    return clickRecordsOfUsers
  }
}
