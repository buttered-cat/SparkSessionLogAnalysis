package SessionAnalysis

import org.apache.spark.SparkContext

/**
  * Created by Administrator on 2017/7/5.
  */
object RegisterSessionAccumulators {
  def run(sc: SparkContext): SessionStatAccumulator =
  {
    val sessionStatAcc = new SessionStatAccumulator()
    sc.register(sessionStatAcc, "sessionStat")
    return sessionStatAcc
  }
}
