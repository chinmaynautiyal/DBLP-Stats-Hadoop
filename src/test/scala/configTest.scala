import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.scalatest.FlatSpec


/**
 * Check the correctness of config files
 */

class configTest extends FlatSpec {
  val configuration = new Configuration
  val conf = ConfigFactory.load()
  val configList = conf.getConfigList("jobs")




    "configList size" should "be greater than 0" in {
      val size = configList.size
      assert(size> 0)

    }

  "configList items " should "have ids" in {
    val id = configList.get(0)
    assert(id.toString.length > 0)

  }



  "Load DTD" should "Load the dtd's path as the URI" in {
    val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI
    assert(dtdFilePath.toString.length > 0)

  }




}