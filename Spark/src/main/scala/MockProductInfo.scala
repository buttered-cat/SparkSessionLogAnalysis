import java.io.{File, PrintWriter}

import scala.util.Random

/**
  * Created by myapple on 17/3/10.
  */
object MockProductInfo {

  val productStatus = Array(0, 1)

  val writer = new PrintWriter(new File("C:\\a\\product.txt"))

  val random = new Random()

  val numProduct = 5000

  def generateProductInfo(): Unit = {

    for (i <- 0 to numProduct) {

      val productID = i

      val product_title = "product" + i

      val extendnfo = "{" + "\"product_status\":" + productStatus(random.nextInt(2)) + "}"

      val record = productID + "\t" + product_title + "\t" + extendnfo

      writer.println(record)

    }
    writer.close()
  }
}
