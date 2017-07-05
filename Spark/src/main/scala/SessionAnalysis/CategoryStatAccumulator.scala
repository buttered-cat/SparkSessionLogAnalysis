package SessionAnalysis

import org.apache.spark.{SparkContext, util}
import org.apache.spark.util.AccumulatorV2

/**
  * Created by Administrator on 2017/7/4.
  */

@SerialVersionUID(125L)
class CategoryStatAccumulator(Stat: Map[Int, Array[Int]])
  extends AccumulatorV2[(Int, Int), Map[Int, Array[Int]]] with Serializable
{
  val CLICK = 0
  val ORDER = 1
  val PAY = 2

  var stat: Map[Int, Array[Int]] = Map[Int, Array[Int]]()

  Stat.foreach(pair =>
  {
    stat += ( pair._1 -> Array(pair._2(CLICK), pair._2(ORDER), pair._2(PAY)))
  })

  override def add(categoryIDAction: (Int, Int)): Unit =
  {
    stat(categoryIDAction._1)(categoryIDAction._2) += 1
  }

  override def copy(): CategoryStatAccumulator =
  {
    val newObj = new CategoryStatAccumulator(stat)
    newObj
  }

  override def isZero: Boolean =
  {
    true
  }

  override def merge(other: AccumulatorV2[(Int, Int), Map[Int, Array[Int]]])
  : Unit = other match {
    case map: CategoryStatAccumulator =>
      other.value.foreach(pair =>
      {
        stat(pair._1)(CLICK) += pair._2(CLICK)
        stat(pair._1)(ORDER) += pair._2(ORDER)
        stat(pair._1)(PAY) += pair._2(PAY)
      })

    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")

  }

  override def reset(): Unit =
  {
  }

  override def value: Map[Int, Array[Int]] =
  {
    stat
  }

}
