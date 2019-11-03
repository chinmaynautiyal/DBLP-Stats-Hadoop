

package dblpStats


import utilities.jobConfig
import dblpStats.XmlInputFormatMultipleTags
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import mappers.{authorStatsMapper, authorshipScoreMapper, dblpMapper, stratifyStatsMapper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{FloatWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import reducers.{authorStatsReducer, authorshipScoreReducer, dblpReducer, stratifyStatsReducer}
import utilities.jobConfig

import scala.jdk.CollectionConverters._



object driverMapReduce extends LazyLogging {

  def main(args: Array[String]):Unit = {


    logger.info("starting the map reduce driver")

    if(args.length != 2){
      logger.error("Input output paths are not provided")
      System.exit(-1)
    }

    //setting input and output directories

    val inputdir = args(0)
    val outputdir = args(1)



    //creating job and setting job configuration

    val conf = ConfigFactory.load()
    val jobSettingsList = conf.getConfigList("jobs").asScala




    jobSettingsList.foreach{ i =>
      //for each job parameter initialise according to job ids
      val hdpConfig = new Configuration()

      //object for holding job configuration settings
      val myJobConfig: jobConfig = new jobConfig(i) //pass config list item from configList


      hdpConfig.setStrings(XmlInputFormatMultipleTags.START_TAG_KEY, myJobConfig.xmlInputStartTags:_*)//like settings
      hdpConfig.setStrings(XmlInputFormatMultipleTags.END_TAG_KEY, myJobConfig.xmlEndTags:_*)

      val job = Job.getInstance(hdpConfig, myJobConfig.jobName) //use job name from myJobConfig


      //put switch case for different jobs based on parameters from myJobConfig


      //setting job parameters common to all jobs
      job.setJarByClass(this.getClass)
      job.setInputFormatClass(classOf[XmlInputFormatMultipleTags])
      FileInputFormat.setInputPaths(job, new Path(inputdir))
      FileOutputFormat.setOutputPath(job, new Path(outputdir+ "/" + myJobConfig.jobId.toString))


      myJobConfig.jobId match {

        case 1 => //first job is the counting co authors into bins
          //set mappers and reducer classes for the first job
          job.setMapperClass(classOf[dblpMapper])
          job.setCombinerClass(classOf[dblpReducer])
          job.setReducerClass(classOf[dblpReducer])

          job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
          job.setOutputKeyClass(classOf[Text])
          job.setOutputValueClass(classOf[IntWritable])

        case 2 =>
          //this is the stratification job
          //get year, journal name and conference
          job.setMapperClass(classOf[stratifyStatsMapper])
          job.setCombinerClass(classOf[stratifyStatsReducer])
          job.setReducerClass(classOf[stratifyStatsReducer])


          job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
          job.setOutputKeyClass(classOf[Text])
          job.setOutputValueClass(classOf[IntWritable])

          //set output path



        case 3 =>
          //authorship score job
          job.setMapperClass(classOf[authorshipScoreMapper])
          job.setCombinerClass(classOf[authorshipScoreReducer])
          job.setReducerClass(classOf[authorshipScoreReducer])

          job.setOutputFormatClass(classOf[TextOutputFormat[Text, FloatWritable]])
          job.setOutputKeyClass(classOf[Text])
          job.setOutputValueClass(classOf[FloatWritable])

        case 4 =>
          //max median average job
          job.setMapperClass(classOf[authorStatsMapper])
          //job.setCombinerClass(classOf[authorStatsReducer])
          job.setReducerClass(classOf[authorStatsReducer])

          //set one reducer
          job.setNumReduceTasks(1)


          //setting mapper output
          job.setMapOutputKeyClass(classOf[Text])
          job.setMapOutputValueClass(classOf[IntWritable])


          job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
          job.setOutputKeyClass(classOf[Text])
          job.setOutputValueClass(classOf[Text])
        case 5 =>
          //stratifying max median job
          job.setMapperClass(classOf[authorStatsMapper])
          job.setCombinerClass(classOf[dblpReducer])
          job.setReducerClass(classOf[dblpReducer])

          job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
          job.setOutputKeyClass(classOf[Text])
          job.setOutputValueClass(classOf[IntWritable])

        case _=>
          print("unconfigured job")


      }

      //set input and output paths




      logger.info("Submitting job")
      job.waitForCompletion(true)
      logger.info("Job completed. Moving to next job")




    }
    logger.info("Completed All jobs. No more jobs in configuration file.")




  }







}



