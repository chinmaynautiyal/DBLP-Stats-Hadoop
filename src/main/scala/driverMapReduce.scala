

package dblpStats

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat



object driverMapReduce extends LazyLogging {

  def main(args: Array[String]):Unit = {

    logger.info("starting the map reduce driver")
    //creating job and setting job configuration

    val MRjobConfig = new Configuration()
    //importing the configuration for the start and end tags, from the configuration file
    val conf = ConfigFactory.load("tagConfig")







  }


}
