package SessionAnalysis

import java.util.Date

import DB._

/**
  * Created by Administrator on 2017/7/6.
  */
class Task(task: MyTask) {
  val taskID = task.taskID
  val taskStartTime = task.taskStartTime

  val ageLo: Option[Int] =
  {
    if (task.ageLo != -1)
      Some(task.ageLo)
    else None
  }

  val ageHi: Option[Int] =
  {
    if (task.ageHi != -1)
      Some(task.ageHi)
    else None
  }

  val professions: Option[Set[String]] =
  {
    if(task.professions != null)
    {
      var set = Set[String]()
      for (i <- 0 until task.professions.size())
        set += task.professions.get(i)

      Some(set)
    }
    else None
  }

  val cities: Option[Set[String]] =
  {
    if(task.cities != null)
    {
      var set = Set[String]()
      for (i <- 0 until task.cities.size())
        set += task.cities.get(i)

      Some(set)
    }
    else None
  }

  val keywords: Option[Set[String]] =
  {
    if(task.keywords != null)
    {
      var set = Set[String]()
      for (i <- 0 until task.keywords.size())
        set += task.keywords.get(i)

      Some(set)
    }
    else None
  }

  val categoryIDs: Option[Set[String]] =
  {
    if(task.categoryIDs != null)
    {
      var set = Set[String]()
      for (i <- 0 until task.categoryIDs.size())
        set += task.categoryIDs.get(i).toString

      Some(set)
    }
    else None
  }

  val sessionStartFrom: Option[Date] =
  {
    if (task.sessionStartFrom != null)
      Option(task.sessionStartFrom)
    else None
  }

  val sessionUntil: Option[Date] =
  {
    if (task.sessionUntil != null)
      Option(task.sessionUntil)
    else None
  }

}
