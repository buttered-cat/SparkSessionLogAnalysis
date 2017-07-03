/**
  * Created by Administrator on 2017/6/23.
  */
object ClosureTest extends App{
  def multiplier(time: Int) = (y: String) => y * time
  val closure1 = multiplier(1)
  val closure2 = multiplier(4)
  println(closure1("hello"))
  println(closure2("hello"))
}

