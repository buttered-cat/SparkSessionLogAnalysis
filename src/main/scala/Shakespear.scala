import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{TextInputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
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

    val conf = new SparkConf().setAppName("Shakespear").set("spark.shuffle.consolidateFiles", "true")
    val sc = new SparkContext(conf)

    val startTime = System.currentTimeMillis()

    val numPartition = 15
    val inputFile = sc.textFile(inputPath)
      .persist(StorageLevel.MEMORY_ONLY).
      coalesce(numPartition, false)

//    val inputFile = sc.newAPIHadoopFile[Text, LongWritable, CombineTextInputFormat](inputPath)
//    inputFile.saveAsNewAPIHadoopFile(outputPath, classOf[LongWritable], classOf[Text],classOf[TextOutputFormat[LongWritable,Text]], conf)

    val stopwords = sc.broadcast(getStopWords())

//    val blankLineSum = inputFile.filter(line => line == "").count()
    val blankLineSum = sc.accumulator(0)

    val ckpt1 = System.currentTimeMillis()

    val res = inputFile.filter(line => {
      if(line == "") {
        blankLineSum.add(1)
        false
      } else true
    })
      .flatMap(getWords(_))
      .filter(word => !stopwords.value.contains(word.toLowerCase))
      .map(word => (word, 1))
      .reduceByKey(_+_)
//      .map(wordPair => (wordPair._2._2, wordPair._2._1))
      .sortBy(_._2, false).take(100)

    val stopTime = System.currentTimeMillis()

    println(blankLineSum)
    res.foreach(println(_))
    println(ckpt1 - startTime)
    println(stopTime - ckpt1)
    println(stopTime - startTime)

    sc.stop()
  }

    def getWords(str: String) =
    {
      val pattern = "[a-zA-Z]+".r
      pattern.findAllIn(str).toArray
//        .map(word => (word, word.toLowerCase))
    }

    def getStopWords() =
    {
      val stopwords = Source.fromInputStream(Shakespear.getClass.getClassLoader.getResourceAsStream("stopword.txt")).getLines()
      .map(_.trim()).toList
      //      stopwords ::: stopwords.map(_.capitalize) ::: stopwords.map(_.toUpperCase)
      stopwords
    }

}
