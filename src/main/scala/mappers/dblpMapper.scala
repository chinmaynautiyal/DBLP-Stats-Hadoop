package mappers

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper

import scala.xml.XML


/**
 * mapper for the first task,
 * maps number of co-authors to one
 *
 */


class dblpMapper extends Mapper[LongWritable, Text, Text, IntWritable] with LazyLogging {



  /*resolved
  //do I need a settings object here?
  //just need job parameters: name, id, xml-start-tags?, ..

  */



  val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI
  //val dtdFilePath = conf.getString("dblp-dtd-path")
  val one = new IntWritable(1)
  val xmlParser = SAXParserFactory.newInstance().newSAXParser()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {

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

    if(authorList.size > 0)
      context.write(new Text(authorList.size.toString), one )






  }


}
