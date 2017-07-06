package SessionAnalysis

import java.text.SimpleDateFormat
import java.util.Date
import PublicVals._

import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2017/7/3.
  *
  * Inclusive bound
  * 目前每条session记录只有一个关键词
  *
  */

class SessionFilterCondition(StartTime: Option[Date] = None, EndTime: Option[Date] = None
                             , Keywords: Option[Set[String]] = None, Categories: Option[Set[String]] = None)
  extends Serializable
{
//  val users = Users
  val startTime = StartTime
  val endTime = EndTime
  val keywords = Keywords
  val categories = Categories

  def isValidClick(click: Array[String], sessionStatAcc: SessionStatAccumulator): Boolean =
  {
    if(startTime isDefined)
    {
      val datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      if(datetimeFormat.parse(click(sessionRawIndex("actionTime"))).compareTo(startTime.get) < 0) return false
    }
    if(endTime isDefined)
    {
      val datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      if(datetimeFormat.parse(click(sessionRawIndex("actionTime"))).compareTo(endTime.get) > 0) return false
    }
    if(keywords isDefined)
      if(!(keywords.get contains click(sessionRawIndex("keywords")))) return false
    if(categories isDefined)
      if(sessionStatAcc.sessionClickCategoryContains(
        click(sessionRawIndex("sessionID")), categories.get)
      )
        return false
    return true
  }

}
