package reducers

import java.lang

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Reducer

import scala.jdk.CollectionConverters._

class stratifyStatsReducer extends Reducer[Text, IntWritable, Text, IntWritable] with LazyLogging{

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {


    val sum = values.asScala.foldLeft(0)(_ + _.get)
    logger.info("Reducer output generated")
    context.write(key, new IntWritable((sum)))
  }




}
