import java.io.{File, PrintWriter}
import scala.util.Random

/**
  * Created by myapple on 17/3/10.
  */
object MockUserInfo {

  val phone = Array("136", "152", "186", "130", "132", "133", "134", "151", "134", "135",
    "136", "150", "156", "158")

  val sexs = Array("male", "female")

  val numUser = 10000

  val random = new Random()

  val start = 10000000

  val writer = new PrintWriter(new File("C:\\a\\user.txt"))

  def generateUserInfo(): Unit = {

    for (i <- 0 to numUser) {

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

      writer.println(record)

    }
    writer.close()
  }
}

