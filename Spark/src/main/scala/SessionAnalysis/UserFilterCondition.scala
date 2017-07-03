package SessionAnalysis

/**
  * Created by Administrator on 2017/7/3.
  */

@SerialVersionUID(123L)
class UserFilterCondition(AgeLo: Option[Int] = None, AgeHi: Option[Int] = None
                          , Professions: Option[Set[String]] = None, Cities: Option[Set[String]] = None) extends Serializable
{
  val ageLo: Option[Int] = AgeLo
  val ageHi: Option[Int] = AgeHi
  val professions: Option[Set[String]] = Professions
  val cities: Option[Set[String]] = Cities

  override def toString: String = super.toString

  def isValidUser(user: Array[String]): Boolean =
  {
    if(ageLo isDefined)
      if(user(3).toInt < ageLo.get) return false
    if(ageHi isDefined)
      if(user(3).toInt > ageHi.get) return false
    if(professions isDefined)
      if(!(professions.get contains user(4))) return false
    if(cities isDefined)
      if(!(cities.get contains user(5))) return false
    return true
  }
}
