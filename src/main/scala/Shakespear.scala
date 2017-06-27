import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by Administrator on 2017/6/26.
  */
object Shakespear {
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)
//    val stopWordPath = args(2)

    val conf = new SparkConf().setAppName("Shakespear")
    val sc = new SparkContext(conf)

    val startTime = System.currentTimeMillis()

    val inputFile = sc.textFile(inputPath).persist(StorageLevel.MEMORY_ONLY)
    val stopwords = sc.broadcast(getStopWords())

//    val blankLineSum = inputFile.filter(line => line == "").count()
    val blankLineSum = sc.accumulator(0)

    val res = inputFile.filter(line => {
      if(line == "") {
        blankLineSum.add(1)
        return false
      }
      return true
    }).flatMap(getWords(_))
      .filter(wordPair => !stopwords.value.contains(wordPair._2)).map(wordPair => (wordPair._2, (1, wordPair._1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2)).map(wordPair => (wordPair._2._2, wordPair._2._1))
      .sortBy(_._2, false).take(100)

    val stopTime = System.currentTimeMillis()

    println(blankLineSum)
    res.foreach(println(_))
    println(stopTime - startTime)

    sc.stop()
  }

    def getWords(str: String) =
    {
      val pattern = "[a-zA-Z]+".r
      pattern.findAllIn(str).toArray.map(word => (word, word.toLowerCase))
    }

    def getStopWords() =
    {
      val stopwords = Source.fromInputStream(Shakespear.getClass.getClassLoader.getResourceAsStream("stopword.txt")).getLines()
        .map(_.trim()).toList
//      stopwords ::: stopwords.map(_.capitalize) ::: stopwords.map(_.toUpperCase)
      stopwords
    }

}
