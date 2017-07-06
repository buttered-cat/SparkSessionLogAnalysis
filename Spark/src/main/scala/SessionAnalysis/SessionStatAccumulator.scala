package SessionAnalysis

import java.text.SimpleDateFormat

import org.apache.spark.util.AccumulatorV2
import PublicVals._
import java.util.Date

import scala.collection.mutable

/**
  * Created by Administrator on 2017/7/5.
  */
class SessionStatAccumulator extends AccumulatorV2[(String, String), Map[String, Array[String]]] with Serializable
{
  val STEP = 0
  val EARLIEST_TYME = 1
  val LATEST_TYME = 2

  val DURATION_0_TO_3S = 0
  val DURATION_4_TO_6S = 1
  val DURATION_7_TO_9S = 2
  val DURATION_10_TO_30S = 3
  val DURATION_31_TO_60S = 4
  val DURATION_OTHERS = 5

  val STEP_1_TO_3 = 6
  val STEP_4_TO_6 = 7
  val STEP_7_TO_9 = 8
  val STEP_10_TO_30 = 9
  val STEP_31_TO_60 = 10
  val STEP_OTHERS = 11



  var stat: Map[String, Array[String]] = Map[String, Array[String]]()
  var clickCategories: mutable.Map[String, Set[String]] = mutable.Map[String, Set[String]]()

  override def add(sessionIDAction: (String, String)): Unit =
  {
    if( sessionIDAction._2 == "" )
      throw new Exception

    if(stat.get(sessionIDAction._1) isDefined)
    {
      stat(sessionIDAction._1)(STEP) = (stat(sessionIDAction._1)(STEP).toInt + 1).toString

      if( stat(sessionIDAction._1)(EARLIEST_TYME) == ""
        || stat(sessionIDAction._1)(LATEST_TYME) == "")
        throw new Exception

      else
      {
        val datetimeFormat1 = new SimpleDateFormat(datetimeFormatStr)
        val datetimeFormat2 = new SimpleDateFormat(datetimeFormatStr)
        val datetimeFormat3 = new SimpleDateFormat(datetimeFormatStr)

        var earliestTime = datetimeFormat1.parse( stat(sessionIDAction._1)(EARLIEST_TYME) )
        var latestTime = datetimeFormat2.parse( stat(sessionIDAction._1)(LATEST_TYME) )
        var curTime = datetimeFormat3.parse(sessionIDAction._2)

        if(earliestTime.compareTo(curTime) > 0)
          stat(sessionIDAction._1)(EARLIEST_TYME) = sessionIDAction._2
        else if(latestTime.compareTo(curTime) < 0)
          stat(sessionIDAction._1)(LATEST_TYME) = sessionIDAction._2
      }

    }

    else
    {
      stat += (sessionIDAction._1 -> Array("1", sessionIDAction._2, sessionIDAction._2))
    }

  }

  def addKeyword(sessionKeyword: (String, String)): Unit =
  {
    if( sessionKeyword._2 == ""
      || sessionKeyword._2 == " " )
      throw new Exception

    if(clickCategories.get(sessionKeyword._1) isDefined)
      clickCategories(sessionKeyword._1) += sessionKeyword._2

    else
      clickCategories += (sessionKeyword._1 -> Set[String]())
  }

  def sumUpStats(): mutable.Map[Int, Int] =
  {
    var allStats = mutable.Map[Int, Int]()
    for (i <- DURATION_0_TO_3S to STEP_OTHERS)
      allStats += (i -> 0)

    stat.foreach(session =>
    {
      val step = session._2(STEP).toInt
      val stepInterval: Int = step match
      {
        case s if s < 4 && s > 0 => STEP_1_TO_3
        case s if s < 7 && s > 3 => STEP_4_TO_6
        case s if s < 10 && s > 6 => STEP_7_TO_9
        case s if s < 31 && s > 9 => STEP_10_TO_30
        case s if s < 61 && s > 30 => STEP_31_TO_60
        case _ => STEP_OTHERS
      }

      allStats(stepInterval) += 1

      val startTime = datetimeFormat.parse(session._2(EARLIEST_TYME))
      val endTime = datetimeFormat.parse(session._2(LATEST_TYME))
      val duration = ( endTime.getTime - startTime.getTime )/* / 1000*/
      val durationInterval = duration match
      {
        case d if d < 4000 => DURATION_0_TO_3S
        case d if d < 7000 && d > 3999 => DURATION_4_TO_6S
        case d if d < 10000 && d > 6999 => DURATION_7_TO_9S
        case d if d < 30001 && d > 9000 => DURATION_10_TO_30S
        case d if d < 61000 && d > 29999 => DURATION_31_TO_60S
        case _ => DURATION_OTHERS
      }

      allStats(durationInterval) += 1
    })

    allStats
  }

  def sessionClickCategoryContains(sessionID: String, clickCategoryCond: Set[String]): Boolean =
  {
    val clickCategory = clickCategories.get(sessionID)
    if (clickCategory isDefined)
    {
      if ((clickCategory.get & clickCategoryCond) != clickCategoryCond)
        return false
      else return true
    }
    else throw new Exception("sessionID doesn't exist in click categories.")
//    return false
  }

  override def copy(): SessionStatAccumulator =
  {
    val newObj = new SessionStatAccumulator()
    stat.foreach(pair =>
    {
      newObj.stat += ( pair._1 -> Array(pair._2(STEP), pair._2(EARLIEST_TYME), pair._2(LATEST_TYME)))
    })
    newObj
  }

  override def isZero: Boolean =
  {
    true
  }

  override def merge(other: AccumulatorV2[(String, String), Map[String, Array[String]]])
  : Unit = other match {
    case map: SessionStatAccumulator =>
      other.value.foreach(pair =>
      {
        if(stat.get(pair._1) isDefined)
        {
          stat(pair._1)(STEP) = (stat(pair._1)(STEP).toInt + pair._2(STEP).toInt).toString
          var earliestTimeOfThis = datetimeFormat.parse( stat(pair._1)(EARLIEST_TYME) )
          var latestTimeOfThis = datetimeFormat.parse( stat(pair._1)(LATEST_TYME) )
          var earliestTimeOfThat = datetimeFormat.parse( pair._2(EARLIEST_TYME) )
          var latestTimeOfThat = datetimeFormat.parse( pair._2(LATEST_TYME) )

          if(earliestTimeOfThis.compareTo(earliestTimeOfThat) > 0)
            stat(pair._1)(EARLIEST_TYME) = pair._2(EARLIEST_TYME)

          if(latestTimeOfThis.compareTo(latestTimeOfThat) < 0)
            stat(pair._1)(LATEST_TYME) = pair._2(LATEST_TYME)
        }
        else
        {
          stat += (pair._1 -> Array(pair._2(STEP), pair._2(EARLIEST_TYME), pair._2(LATEST_TYME)))
        }
      })

    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def reset(): Unit =
  {
  }

  override def value: Map[String, Array[String]] =
  {
    stat
  }
}
