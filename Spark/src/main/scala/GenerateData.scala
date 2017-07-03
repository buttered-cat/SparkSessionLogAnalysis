package DataGenerate

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import scala.util.Random

/**
  * Created by Administrator on 2017/7/1.
  */
object GenerateData {
  val writerProduct = new PrintWriter(new File("C:\\a\\test\\product.txt"))
  val writerUser = new PrintWriter(new File("C:\\a\\test\\user.txt"))
  val writerCLick = new PrintWriter(new File("C:\\a\\test\\click.log"))
  def main(args: Array[String]): Unit = {
    generateData()
  }
  def generateData(): Unit ={
    // 设置生成的用户数量
    val userNum = 1000
    // 设置生成的点击数量
    val clickNum = 2000000


    /**
      * 先生成用户记录
      */
    // 用户记录用到的数据
    val userArray: Array[String] = new Array[String](userNum+1)

    val random = new Random()
    val start = 10000000
    val phone = Array("136", "152", "186", "130", "132", "133", "134", "151", "134", "135",
      "136", "150", "156", "158")
    val sexs = Array("male", "female")

    for (i <- 0 to userNum){
      val first = phone(random.nextInt(phone.length))
      //生成手机号后8位
      val second = (start + random.nextInt(start)).toString.reverse
      //手机号 == 用户id
      val userID = first + second

      val username = "user" + i

      val name = "name" + i

      val age = random.nextInt(60)

      val profession = "professional" + random.nextInt(100)

      val city = "city" + random.nextInt(100)

      val sex = sexs(random.nextInt(2))

      val record = userID + " " + username + " " + name + " " +
        age + " " + profession + " " + city + " " + sex

      writerUser.println(record)

      userArray(i) = userID
    }

    /**
      * 接着生成网购用户行为的模拟数据
      */

    val searchKeywords = Array("冰箱", "iphone6", "电视", "葡萄干", "尿不湿",
      "耳机", "小米5", "蚊帐", "牛排", "U盘")
    val actions = Array("search", "click", "order", "pay")
    val date = getToday()

    for (i <- 0 to userNum){
      val userID = userArray(i)

      /**
        * 每个用户对应1~·15条不等的session回话链接
        */
      val sessionNum = random.nextInt(15)+1
      for (j <- 0 to sessionNum){

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
          writerCLick.println(record)
        }
      }
    }


    val productStatus = Array(0, 1)
    val numProduct = 5000

    for (i <- 0 to numProduct) {

      val productID = i

      val product_title = "product" + i

      val extendnfo = "{" + "\"product_status\":" + productStatus(random.nextInt(2)) + "}"

      val record = productID + "\t" + product_title + "\t" + extendnfo

      writerProduct.println(record)

    }
    writerCLick.close()
    writerProduct.close()
    writerUser.close()
  }

   def getToday() = {
    val date = new Date()
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val ret = format.format(date)
     ret
  }

}