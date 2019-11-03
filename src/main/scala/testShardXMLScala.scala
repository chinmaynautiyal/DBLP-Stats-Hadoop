

import javax.xml.parsers.SAXParserFactory

import scala.xml.XML






object testShardXMLScala {


  def main(args: Array[String]): Unit = {

    val xmltestShard = XML.loadFile("/Users/chinmay/Downloads/oneEntryTest.xml")
    val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI


    val xmlParser = SAXParserFactory.newInstance().newSAXParser()
    val xmldtdString = s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtdFilePath">
              <dblp>""" + xmltestShard.toString + "</dblp>"


    val validXMLelement = XML.withSAXParser(xmlParser).loadString(xmldtdString)
    val extractAuthors = (validXMLelement \\ "author").map{node =>  node.text.toString.toLowerCase.trim}.toList
    val size = extractAuthors.size

    println("the size of the authorlist is", size.toFloat)

    val defaultScore = (1 / size.toFloat)
    val addOnScore  = 1 / (4 * size.toFloat)


    println("The default score is ", defaultScore)
    println("the add on score is ", addOnScore)

    extractAuthors.indices.foreach { i =>
      if (i == 0) {
        println("first element")
      }
      else if (i == size - 1) {
        println("last element")
      }
      else {
        println(".")
      }
    }

  }







}
