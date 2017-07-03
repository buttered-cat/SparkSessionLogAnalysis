import java.io.{File, PrintWriter}
import java.util.{Date, UUID}
//import com.tosit.project.javautils.DateUtils

import scala.util.Random


object MockClickData {

  /**
    * 产生网购用户行为的模拟数据
    *
    */
  def generateMockClickData(): Unit = {
    /**
      * 生产数据文件
      */
    val writer = new PrintWriter(new File("C:\\a\\click.log"))

    val searchKeywords = Array("冰箱", "iphone6", "电视", "葡萄干", "尿不湿",
      "耳机", "小米5", "蚊帐", "牛排", "U盘")
    val actions = Array("search", "click", "order", "pay")
    //产生随机种子
    val random = new Random()
    //手机号前几位
    val phone = Array("136", "152", "186", "130", "132", "133", "134", "151", "134", "135",
      "136", "150", "156", "158")
    //产生随机8位数
    val start = 10000000
    val date = new Date()
    val numUser = 1000

    /**
      * 生产1000个用户数据
      */
    for (i <- 0 to numUser ) {

      val first = phone(random.nextInt(phone.length))
      //生成手机号后8位
      val second = (start + random.nextInt(start)).toString.reverse
      //手机号 == 用户id
      val userID = first + second

      /**
        * 每个用户对应1~15条不等的session会话链接
        */
      val numSession = random.nextInt(15) + 1
      for (j <- 0.to(numSession)) {

        //定义一次session回话
        val sessiopnID = UUID.randomUUID().toString.replace("-", "")
        //定义session的创建时间，也即是会话开始时间
        val startSessionTime = date + " " + random.nextInt(23)
        var firstClickCategory = 0
        //随机返回访问0~50个页面
        val numPage = random.nextInt(50) + 1

        //定义一次session回话，用户点击1~50个页面
        for (z <- 0 to numPage ) {

          //访问的页面ID
          val pageID = random.nextInt(20)
          val action = actions(random.nextInt(4))
          val actionTime = startSessionTime + ":" +
            random.nextInt(59) + ":" + random.nextInt(59)

          val Array(keywords, clickCategoryId, clickProductId, orderCategoryId,
            orderProductId, payCategoryId, payProductId) = {
            if (action == "search")
              Array(searchKeywords(random.nextInt(10)), " ", " ", " ", " ", " ", " ")
            else if (action == "order")
              Array(" ", " ", " ", random.nextInt(50), random.nextInt(5000) + 1, " ", " ")
            else if (action == "pay")
              Array( " ", " ", " ", " ", " ", random.nextInt(50), random.nextInt(5000) + 1)
            else if (action == "click" && 0 == firstClickCategory) {
              firstClickCategory = random.nextInt(50) + 1
              println("开始访问的商品类型为" + firstClickCategory)
              Array(" ", firstClickCategory,
                random.nextInt(5000) + 1, " ", " ", " ", " ")
            }
            else
              Array(" ", firstClickCategory,random.nextInt(5000) + 1, " ", " ", " ", " ")
          }

          val record = date + "\t" + userID + "\t" + sessiopnID + "\t" + pageID + "\t" +
            actionTime + "\t" + keywords + "\t" + clickCategoryId + "\t" + clickProductId + "\t" +
            orderCategoryId + "\t" + orderProductId + "\t" + payCategoryId + "\t" + payProductId +
            "\t" + random.nextInt(10)
          writer.println(record)

        }

      }

    }
    writer.close()
  }

}
