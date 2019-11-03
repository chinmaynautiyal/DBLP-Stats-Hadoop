package reducers

import java.lang

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Reducer

import scala.jdk.CollectionConverters._

class authorshipScoreReducer extends Reducer[Text, FloatWritable, Text, FloatWritable] with LazyLogging{

  override def reduce(key: Text, values: lang.Iterable[FloatWritable], context: Reducer[Text, FloatWritable, Text, FloatWritable]#Context): Unit = {


    val sum = values.asScala.fold(new FloatWritable(0))((a, b) => new FloatWritable(a.get() + b.get()))
    logger.info("Reducer output generated")
    context.write(key, sum)

  }




}
