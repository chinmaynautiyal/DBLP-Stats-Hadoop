package dblpStats

import java.io.IOException
import java.nio.charset.StandardCharsets

import com.google.common.io.Closeables
import com.typesafe.scalalogging.LazyLogging
import dblpStats.XmlInputFormatMultipleTags.XmlRecordReaderMultipleTags
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{DataOutputBuffer, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

/**
 multi tag XML input
 */
class XmlInputFormatMultipleTags extends TextInputFormat with LazyLogging {

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    try {
      new XmlRecordReaderMultipleTags(split.asInstanceOf[FileSplit], context.getConfiguration)
    }
    catch {
      case ioe: IOException =>
        logger.warn("Error while creating XmlRecordReader", ioe)
        null
    }
  }
}


object XmlInputFormatMultipleTags {


  val START_TAG_KEY = "xmlinput.start"
  val END_TAG_KEY = "xmlinput.end"


  @throws[IOException]
  class XmlRecordReaderMultipleTags(split: FileSplit, conf: Configuration) extends RecordReader[LongWritable, Text] {

    private val startTags = conf.getStrings(START_TAG_KEY).map(_.getBytes(StandardCharsets.UTF_8))
    private val endTags = conf.getStrings(END_TAG_KEY).map(_.getBytes(StandardCharsets.UTF_8))


    private val startTagToEndTagMapping = startTags.zip(endTags).toMap


    private val start = split.getStart
    private val end = start + split.getLength

    private val fsin = split.getPath.getFileSystem(conf).open(split.getPath)
    fsin.seek(start)


    private val buffer = new DataOutputBuffer()


    private val currentKey = new LongWritable()
    private val currentValue = new Text()


    private var matchedTag = Array[Byte]()












    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}

    override def getCurrentKey: LongWritable = {
      new LongWritable(currentKey.get())
    }

    override def getCurrentValue: Text = {
      new Text(currentValue)
    }

    override def getProgress: Float = (fsin.getPos - start) / (end - start).toFloat

    override def close(): Unit = Closeables.close(fsin, true)







    override def nextKeyValue(): Boolean = {
      readNextVal(currentKey, currentValue)
    }


    @throws[IOException]
    private def readNextVal(key: LongWritable, value: Text): Boolean = {


      if (fsin.getPos < end && readTillEnd(startTags, false)) {
        try {

          buffer.write(matchedTag)


          if (readTillEnd(Array(startTagToEndTagMapping(matchedTag)), true)) {


            key.set(fsin.getPos)
            value.set(buffer.getData, 0, buffer.getLength)
            return true
          }
        }
        finally {

          buffer.reset()
        }
      }
      false
    }


    private def readTillEnd(tags: Array[Array[Byte]], lookingForEndTag: Boolean): Boolean = {

      val matchCounter: Array[Int] = tags.indices.map(_ => 0).toArray

      while (true) {

        val currentByte = fsin.read()


        if (currentByte == -1) {
          return false
        }


        if (lookingForEndTag) {
          buffer.write(currentByte)
        }


        tags.indices.foreach { tagIndex =>

          val tag = tags(tagIndex)

          if (currentByte == tag(matchCounter(tagIndex))) {
            matchCounter(tagIndex) += 1


            if (matchCounter(tagIndex) >= tag.length) {
              matchedTag = tag
              return true
            }
          }
          else {

            matchCounter(tagIndex) = 0
          }
        }

        if (!lookingForEndTag && matchCounter.forall(_ == 0) && fsin.getPos >= end) {
          return false
        }
      }
      false
    }


  }

}