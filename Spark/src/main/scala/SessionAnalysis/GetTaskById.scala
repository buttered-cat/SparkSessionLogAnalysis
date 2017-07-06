package SessionAnalysis

import DB._

/**
  * Created by Administrator on 2017/7/6.
  */
object GetTaskById {
  def run(taskID: Int): Task =
  {
    val DAO = DAOFactory.getTaskDAO()
    val task = DAO.findById(taskID)
    new Task(task)
  }
}
