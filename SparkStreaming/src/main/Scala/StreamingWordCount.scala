import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/6/29.
  */
object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Streaming Word Count").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(4))

    val lines = ssc.socketTextStream("192.168.26.101", 9999)
    val words = lines.flatMap(_.split("\\s+"))
    val pairs = words.map(word => (word, 1))
    val count = pairs.reduceByKey(_+_)

    count.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
