package mappers

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper

import scala.xml.XML


/**
 *mapper for counting authorship scores, for producing statistics for top 100 and bottom 100 authors
 */


class authorshipScoreMapper extends Mapper[LongWritable, Text, Text, FloatWritable] with LazyLogging {



  /*resolved
  //do I need a settings object here?
  //just need job parameters: name, id, xml-start-tags?, ..

  */




  val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI
  //val dtdFilePath = conf.getString("dblp-dtd-path")
  //val one = new IntWritable(1)


  val xmlParser = SAXParserFactory.newInstance().newSAXParser()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, FloatWritable]#Context): Unit = {

    //wrap in dtd
    val xmldtdString = s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtdFilePath">
              <dblp>""" + value.toString + "</dblp>"


    val validXMLelement = XML.withSAXParser(xmlParser).loadString(xmldtdString)



    val searchTag = validXMLelement.child.head.label match{
      case "book" | "proceedings" => "editor"
      case _ => "author"

    }
    val authorList = (validXMLelement \\ searchTag).map{authorNodes => authorNodes.text.toLowerCase.trim}.toList

    //valid XML is a based on one entry, so just count the data for different scores
    //the top author gets +1/4n of the

    val numberOfAuthors = authorList.size
    logger.info(s"The number of authors are: "+ numberOfAuthors.toString)
    if (numberOfAuthors != 0) {
      val defaultScore = 1 / numberOfAuthors.toFloat



      logger.info(s"This entry has: number of authors => " +  numberOfAuthors)
      logger.info(s"The default score is: " +  defaultScore)


      val addOnScore = 1 / (4 * numberOfAuthors.toFloat)
      logger.trace(s"The add on score is =>" + addOnScore.toString)
      authorList.indices.foreach { i =>
        if (i == 0) {

          logger.info(s"The first guy get => "  + (defaultScore+addOnScore).toString)
          context.write(new Text(authorList(i)), new FloatWritable(defaultScore + addOnScore))
        }
        else if (i == numberOfAuthors - 1) {
          logger.info("The last guy gets => " +  (defaultScore-addOnScore).toString)
          context.write(new Text(authorList(i)), new FloatWritable(defaultScore - addOnScore))
        }
        else {
          logger.info("Everybody else gets => ", defaultScore+addOnScore)
          context.write(new Text(authorList(i)), new FloatWritable(defaultScore))
        }
      }

    }







  }

}
