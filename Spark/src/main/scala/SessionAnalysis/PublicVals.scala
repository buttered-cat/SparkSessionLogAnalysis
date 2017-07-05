package SessionAnalysis

import java.text.SimpleDateFormat

import org.apache.spark.SparkContext

/**
  * Created by Administrator on 2017/7/4.
  */
object PublicVals {
  val sessionRawIndex = Map(
    "sessionActionID" -> 0,
    "date" -> 1,
    "userID" -> 2,
    "sessionID" -> 3,
    "pageID" -> 4,
    "actionTime" -> 5,
    "keywords" -> 6,
    "clickCategoryID" -> 7,
    "clickProductID" -> 8,
    "orderCategoryID" -> 9,
    "orderProductID" -> 10,
    "payCategoryID" -> 11,
    "payProductID" -> 12,
    "reservedField" -> 13
  )

  val sessionRDDIndex = Map(
    "sessionID" -> 0,
    "sessionActionID" -> 1,
    "date" -> 2,
    "pageID" -> 3,
    "actionTime" -> 4,
    "keywords" -> 5,
    "clickCategoryID" -> 6,
    "clickProductID" -> 7,
    "orderCategoryID" -> 8,
    "orderProductID" -> 9,
    "payCategoryID" -> 10,
    "payProductID" -> 11,
    "reservedField" -> 12
  )

  val userRDDIndex = Map(
    "username" -> 0,
    "name" -> 1,
    "age" -> 2,
    "profession" -> 3,
    "city" -> 4,
    "gender" -> 5
  )

  val finalSessionMapIndex = Map(
    "sessionID" -> 0,
    "keywords" -> 1,
    "clickCategories" -> 2,
    "age" -> 3,
    "profession" -> 4,
    "city" -> 5,
    "gender" -> 6
  )

  val datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val datetimeFormatStr = "yyyy-MM-dd HH:mm:ss"

  def getCategoryDict(sc: SparkContext, DataPath: String): Map[Int, String] =
  {
    val DEBUG = true
    val dataPath = {if(DEBUG) "C:\\a\\test\\category.txt" else DataPath}

    val categoryDict: Map[Int, String] = sc.textFile(dataPath)
      .map(line => line.split("\t")(0).toInt -> line.split("\t")(1)).collect().toMap
    categoryDict
  }

}
