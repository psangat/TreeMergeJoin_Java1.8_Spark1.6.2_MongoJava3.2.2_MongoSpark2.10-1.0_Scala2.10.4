import java.io.File
import java.util
import java.util.Scanner

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import org.json.JSONObject

import scala.collection.JavaConversions._

/**
  * Created by psangats on 28/08/2016.
  */
object InsertTest {
  val _40KRecordsList = new util.ArrayList[util.ArrayList[String]]()

  def main(args: Array[String]) {
    // issue#1: java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
    System.setProperty("hadoop.home.dir", "C:\\Applications\\")
    // end issue#1
    val conf = new SparkConf().setAppName("TreeMerge").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val writeConf = WriteConfig(Map("uri" -> "mongodb://127.0.0.1:27017/Monash.InsertTest"))

    create40kRecordsBatch("C:\\\\Raw_Data_Source_For_Test\\\\AllFiles\\\\")

    println("TEst")
    println(_40KRecordsList.size())
    for (i <- 0 until _40KRecordsList.size()) {
      val rdd = sc.parallelize(_40KRecordsList.get(i)).map(j => Document.parse(j))

      val startTime = System.currentTimeMillis()
      MongoSpark.save(rdd, writeConf)
      val endTime = System.currentTimeMillis()
      println("Batch " + i)
      println("Time Taken " + (endTime - startTime) + " milli seconds ")
      //Thread.sleep(1000)
    }
  }

  def create40kRecordsBatch(dirPath: String) {
    val filteredFiles = new File(dirPath).listFiles.filter(_.getName.matches("[0-9]{1,4}_(.*?)_(.*?)_ALLOUT(.*?).CSV"))
    val _40kBatch = new util.ArrayList[String]()
    filteredFiles.foreach {
      file =>
        //println("Processing: " + file.getName)
        val sc = new Scanner(file)
        sc.nextLine() // skip header
        while (sc.hasNextLine()) {
          val arrayOfElements = sc.nextLine().split(',')
          if (_40kBatch.size() == 40000) {
            _40KRecordsList.add(_40kBatch.clone().asInstanceOf[util.ArrayList[String]])
            // val printWriter = new PrintWriter(dirPath + "ConvertedToJSON.txt")
            _40kBatch.clear()

          }
          _40kBatch.add(prepareJSONObject(arrayOfElements))
        }
        sc.close()
    }
  }

  def prepareJSONObject(arrayOfElemets: Array[String]): String = {
    val jSONObject = new JSONObject()
    jSONObject.put("somattime", arrayOfElemets(2))
    jSONObject.put("kmh", arrayOfElemets(3))
    jSONObject.put("carorient", arrayOfElemets(4))



    val jSONObjectCFA = new JSONObject()
    jSONObjectCFA.put("min", arrayOfElemets(10))
    jSONObjectCFA.put("max", arrayOfElemets(11))
    jSONObject.put("cfa", jSONObjectCFA)

    val jSONObjectACC = new JSONObject()
    jSONObjectACC.put("r3", arrayOfElemets(12))
    jSONObjectACC.put("r4", arrayOfElemets(13))
    jSONObject.put("acc", jSONObjectACC)

    jSONObject.put("trackname", arrayOfElemets(14))
    jSONObject.put("direction", arrayOfElemets(15))
    jSONObject.put("trackkm", arrayOfElemets(1))

    val jSONObjectGPS = new JSONObject()
    jSONObjectGPS.put("lat", arrayOfElemets(22))
    jSONObjectGPS.put("lon", arrayOfElemets(23))
    jSONObject.put("gps", jSONObjectGPS)
    jSONObject.toString()
  }
}
