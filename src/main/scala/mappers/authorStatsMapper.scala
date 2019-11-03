package mappers

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper

import scala.xml.XML


/**
 * mapper for computing max median and average number of co authors for each author
 *
 *
 * maps author -> number of co authors
 */


class authorStatsMapper extends Mapper[LongWritable, Text, Text, IntWritable] with LazyLogging {




  val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI
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

    if(authorList.size > 0) {

      authorList.foreach {i =>
        //i => context.write(new Text(i + ',' + authorList.size.toString), one)

          context.write(new Text(i), new IntWritable(authorList.size))
      }

    }







  }


}
