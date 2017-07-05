package DataGenerate

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, UUID}

import scala.util.Random

/**
  * Created by Administrator on 2017/7/1.
  */
object GenerateData {
  val writerCategory = new PrintWriter(new File("C:\\a\\test\\category.txt"))
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
    var sessionActionID = 0

    //懒了没写完...
    val monthsWith31Days = Set(1,3,5,7,8,10,12)
    //    val monthsWith30Days = Set(1,3,5,7,8,10,12)



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




    val categoryStatus = Array(0, 1)
    val numCategories = 100

    for (i <- 0 until numCategories) {

      val categoryID = i

      val category_title = "category" + i

      val extendnfo = "{" + "\"category_status\":" + categoryStatus(random.nextInt(2)) + "}"

      val record = categoryID + "\t" + category_title + "\t" + extendnfo

      writerCategory.println(record)

    }








    val productStatus = Array(0, 1)
    val numProduct = 5000

    for (i <- 0 until numProduct) {

      val productID = i

      val product_title = "product" + i

      val category_id = random.nextInt(numCategories)

      val extendnfo = "{" + "\"product_status\":" + productStatus(random.nextInt(2)) + "}"

      val record = productID + "\t" + category_id + "\t" + product_title + "\t" + extendnfo

      writerProduct.println(record)

    }



    /**
      * 接着生成网购用户行为的模拟数据
      */

    val searchKeywords = Array("冰箱", "iphone6", "电视", "葡萄干", "尿不湿",
      "耳机", "小米5", "蚊帐", "牛排", "U盘")
    val actions = Array("search", "click", "order", "pay", "browseCategory")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val actionTimeFormat = new SimpleDateFormat("HH:mm:ss")

    for (i <- 0 to userNum){
      val userID = userArray(i)

      /**
        * 每个用户对应1~15条不等的session记录
        */
      val sessionNum = random.nextInt(15)+1
      for (j <- 0 to sessionNum){
        val date = {
          var c = Calendar.getInstance()
          c.set(Calendar.YEAR, 2014 + random.nextInt(4))        //[ , )
          c.set(Calendar.MONTH, 1 + random.nextInt(12))
          c.set(Calendar.DAY_OF_MONTH, 1 + random.nextInt(28))      //懒了没写完...
          dateFormat.format(c.getTime)
        }

        var time = Calendar.getInstance()
        time.set(Calendar.HOUR, random.nextInt(24))
        time.set(Calendar.MINUTE, random.nextInt(60))

        //定义一次session回话
        val sessionID = UUID.randomUUID().toString.replace("-", "")
        var firstClickCategory = 0
        //随机返回访问0~50个页面
        val numPage = random.nextInt(50) + 1

        //定义一次session回话，用户点击1~50个页面
        for (z <- 0 to numPage ) {

          //访问的页面ID
          val pageID = random.nextInt(20)
          val action = actions(random.nextInt(5))

          time.set(Calendar.SECOND, time.get(Calendar.SECOND) + random.nextInt(10))
          val actionTime = date + " " + actionTimeFormat.format(time.getTime)

          val Array(keywords, clickCategoryID, clickProductID, orderCategoryID,
          orderProductID, payCategoryID, payProductID) =
          {
            if (action == "search")
              Array(searchKeywords(random.nextInt(10)), " ", " ", " ", " ", " ", " ")
            else if (action == "order")
              Array(" ", " ", " ", random.nextInt(numCategories), random.nextInt(numProduct), " ", " ")
            else if (action == "pay")
              Array( " ", " ", " ", " ", " ", random.nextInt(numCategories), random.nextInt(numProduct))
            else if (action == "click") {
              //              println("开始访问的商品类型为" + firstClickCategory)
              firstClickCategory = random.nextInt(numCategories)
              Array(" ", firstClickCategory,
                random.nextInt(numProduct), " ", " ", " ", " ")
            }
            else
            {
              firstClickCategory = random.nextInt(numCategories)
              Array(" ", firstClickCategory, " ", " ", " ", " ", " ")
            }
          }

          val record = sessionActionID + "\t" + date + "\t" + userID + "\t" + sessionID + "\t" + pageID + "\t" +
            actionTime + "\t" + keywords + "\t" + clickCategoryID + "\t" + clickProductID + "\t" +
            orderCategoryID + "\t" + orderProductID + "\t" + payCategoryID + "\t" + payProductID +
            "\t" + random.nextInt(10)
          writerCLick.println(record)
          sessionActionID = sessionActionID + 1
        }
      }
    }


    writerCLick.close()
    writerProduct.close()
    writerUser.close()
    writerCategory.close()

  }

  def getToday() = {
    val date = new Date()
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val ret = format.format(date)
    ret
  }

}