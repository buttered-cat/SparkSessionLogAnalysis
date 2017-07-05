package SessionAnalysis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import PublicVals._

/**
  * Created by Administrator on 2017/7/3.
  *
  *
  *
  *
  */
object SessionFilter {
  val DEBUG = true

  def run(sc: SparkContext, input: String, output: String
          , cond: SessionFilterCondition
          , users: RDD[(String, Array[String])]
          , categoryStatAcc: CategoryStatAccumulator
          , sessionStatAcc: SessionStatAccumulator
         ): RDD[(String, String)] =
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
    val knownKeywords = Set[String]()
    val knownCategories = Set[String]()
    val categoryDict = getCategoryDict(sc, "")

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

      .join(users)

      .map(rec =>
        (
          rec._2._1(sessionRDDIndex("sessionID")),
          Array(rec._2._1(sessionRDDIndex("sessionID"))
            , {
              if(rec._2._1(sessionRDDIndex("keywords")) == " ")
                ""
              else rec._2._1(sessionRDDIndex("keywords"))
            }
            , {
              if(rec._2._1(sessionRDDIndex("clickCategoryID")) != " ")
                categoryDict( rec._2._1(sessionRDDIndex("clickCategoryID")).toInt )
              else ""
            }
            , rec._2._2(userRDDIndex("age"))
            , rec._2._2(userRDDIndex("profession"))
            , rec._2._2(userRDDIndex("city"))
            , rec._2._2(userRDDIndex("gender"))
          )
        )
      )

      .reduceByKey((s1, s2) =>
        {


          Array(
            s1( finalSessionMapIndex("sessionID") )
            , s1( finalSessionMapIndex("keywords") ) +
              {
                {
                  if(s1( finalSessionMapIndex("keywords") ) != ""
                    && s2( finalSessionMapIndex("keywords") ) != "") ","
                  else ""
                } + s2( finalSessionMapIndex("keywords") )
              }
            , s1( finalSessionMapIndex("clickCategories") ) +
              {
                { if(s1( finalSessionMapIndex("clickCategories") ) != ""
                    && s2( finalSessionMapIndex("clickCategories") ) != "") ","
                  else ""
                } + s2( finalSessionMapIndex("clickCategories") )
              }
            , s1( finalSessionMapIndex("age") )
            , s1( finalSessionMapIndex("profession") )
            , s1( finalSessionMapIndex("city") )
            , s1( finalSessionMapIndex("gender") )
          )
        }
      )
      .map(session => (
        session._1,
        "sessionid=" + session._2( finalSessionMapIndex("sessionID") )
          + "|" + "searchword=" + session._2( finalSessionMapIndex("keywords") )
          + "|" + "clickcaterory=" + session._2( finalSessionMapIndex("clickCategories") )
          + "|" + "age=" + session._2( finalSessionMapIndex("age") )
          + "|" + "profession=" + session._2( finalSessionMapIndex("profession") )
          + "|" + "city=" + session._2( finalSessionMapIndex("city") )
          + "|" + "gender=" + session._2( finalSessionMapIndex("gender") )
      ))

    return sessionRecords
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
