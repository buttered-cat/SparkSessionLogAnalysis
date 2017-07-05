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
          , users: RDD[(String, Array[String])]
          , categoryStatAcc: CategoryStatAccumulator
          , sessionStatAcc: SessionStatAccumulator
         ): RDD[(String, (Array[String], Array[String]))] =
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

    val sessionRecords = sc.textFile(inputPath, numProcessors)
      .map(line => line.split("\t"))
      .filter(condition.value isValidClick)
      .filter(aggregateCategory(_, categoryStatAcc))
      .filter(aggregateSessions(_, sessionStatAcc))
      .map(rec => (rec(sessionRawIndex("userID")),
        Array(rec(sessionRawIndex("sessionID"))
          , rec(sessionRawIndex("sessionActionID"))
          , rec(sessionRawIndex("date"))
          , rec(sessionRawIndex("pageID"))
          , rec(sessionRawIndex("actionTime"))
          , rec(sessionRawIndex("keywords"))
          , rec(sessionRawIndex("clickCategoryID"))
          , rec(sessionRawIndex("clickProductID"))
          , rec(sessionRawIndex("orderCategoryID"))
          , rec(sessionRawIndex("orderProductID"))
          , rec(sessionRawIndex("payCategoryID"))
          , rec(sessionRawIndex("payProductID"))
          , rec(sessionRawIndex("reservedField"))
        )))


    val sessionRecordsWithUser = sessionRecords.join(users)

    return sessionRecordsWithUser
  }

  def aggregateCategory(record: Array[String], categoryStatAcc: CategoryStatAccumulator): Boolean =
  {
    if(record(sessionRawIndex("clickCategoryID")) != " ")
      categoryStatAcc.add((record(sessionRawIndex("clickCategoryID")).toInt, categoryStatAcc.CLICK))
    else if(record(sessionRawIndex("orderCategoryID")) != " ")
      categoryStatAcc.add((record(sessionRawIndex("orderCategoryID")).toInt, categoryStatAcc.ORDER))
    else if(record(sessionRawIndex("payCategoryID")) != " ")
      categoryStatAcc.add((record(sessionRawIndex("payCategoryID")).toInt, categoryStatAcc.PAY))
    true
  }

  def aggregateSessions(record: Array[String], sessionStatAcc: SessionStatAccumulator): Boolean =
  {
    sessionStatAcc.add(record(sessionRawIndex("sessionID")), record(sessionRawIndex("actionTime")))
    true
  }
}
