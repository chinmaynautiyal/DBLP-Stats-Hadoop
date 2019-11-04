package dblpStats.utilities

import com.typesafe.config.Config
import scala.jdk.CollectionConverters._

class jobConfig (i: Config){

  val jobName: String = i.getString("name")
  val jobId : Int = i.getString("id").toInt

  val dtdFilePath = i.getString("dblp-dtd-path")

  val xmlInputStartTags: List[String] = i.getStringList("xml-input.start-tags").asScala.toList
  val xmlEndTags: List[String] = i.getStringList("xml-input.end-tags").asScala.toList


  def getYear(): String = {
    i.getString("year")

  }

  def getYearList(): List[String] = {

    i.getStringList("year")
  }.asScala.toList

  def getStratificationFieldList(): List[String] = {

    i.getStringList("stratificationF")
    }.asScala.toList

  def getConferenceList(): List[String] = {

    i.getStringList("conferences")
    }.asScala.toList

}
