package SessionAnalysis

import java.text.SimpleDateFormat

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

  val datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val datetimeFormatStr = "yyyy-MM-dd HH:mm:ss"

}
