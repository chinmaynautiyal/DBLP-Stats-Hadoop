package dblpStats

import dblpStats.utilities.jobConfig
import com.typesafe.config.{Config, ConfigFactory}
import mappers.dblpMapper

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object testingEnv{

  def main(args: Array[String]): Unit = {

    val list = ListBuffer(10, 15, 20 ,30)

    val max = list.max
    val min = list.min
    println("the max is", max)
    println("the min is", min)
    val sortedList = list.sorted
    val mid = sortedList.size / 2


    val median = if (sortedList.size % 2== 1) sortedList(mid) else ((sortedList(mid) + sortedList(mid - 1)) .toFloat/2)

    println("The median is",median)



  }

}