package reducers

import java.lang

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

class authorStatsReducer extends Reducer[Text, IntWritable, Text, Text] with LazyLogging{

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {


    //store values in listbuffer
    val valuesList = new ListBuffer[Integer]


    //iterate over values
    var sum = 0
    values.forEach{ i =>
      valuesList += i.get()
      sum += i.get()
    }



    val avg = sum.toFloat/valuesList.size

    //logger.info("This is for author" + key.toString)
    val sortedList = valuesList.sorted

    val mid = sortedList.size / 2

    val median = if (sortedList.size % 2== 1) sortedList(mid) else ((sortedList(mid) + sortedList(mid - 1) ).toFloat/2)

    val max = sortedList(sortedList.size - 1)
    val min = sortedList(0)
    //logger.info("The min value is" + min.toString)



    //val avg = sum / values.asScala.size.toFloat
    //logger.info("Reducer output generated")
    context.write(key, new Text(min.toString + "," + max.toString + "," + avg.toString + "," + median.toString))
  }




}
