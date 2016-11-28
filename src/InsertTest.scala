import java.io.{File, PrintWriter}
import java.util.{ArrayList, Scanner}

import GPS.GeoHash
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
  val _RecordsList = new ArrayList[ArrayList[String]]()
  val _StatsList = new ArrayList[String]()

  def main(args: Array[String]) {
    // issue#1: java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
    System.setProperty("hadoop.home.dir", "C:\\Applications\\")
    // end issue#1

    val _batchSizes = new ArrayList[Int]()
    _batchSizes.add(10000)
    _batchSizes.add(20000)
    _batchSizes.add(30000)
    _batchSizes.add(40000)
    _batchSizes.add(50000)
    _batchSizes.add(60000)
    _batchSizes.add(70000)
    _batchSizes.add(80000)
    _batchSizes.add(90000)
    _batchSizes.add(100000)
    _batchSizes.add(110000)
    _batchSizes.add(120000)

    val conf = new SparkConf().setAppName("Insert Into Mongo Shrads Test")
      .setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val writeConf = WriteConfig(Map("uri" -> "mongodb://118.138.244.164:27020/CCPE.sensordataNS"))
    //val writeConf = WriteConfig(Map("uri" -> "mongodb://127.0.0.1:27017/Monash.InsertTest1"))
    _batchSizes.foreach { batchSize =>
      println("Batch Size: " + batchSize + " Processing")
      createRecordsBatch("C:\\\\Raw_Data_Source_For_Test\\\\AllFiles\\\\", batchSize)
      for (i <- 0 until _RecordsList.size()) {
        val rdd = sc.parallelize(_RecordsList.get(i)).map(j => Document.parse(j))

        val startTime = System.currentTimeMillis()
        MongoSpark.save(rdd, writeConf)
        val endTime = System.currentTimeMillis()
        println("Batch " + i)
        var time = (endTime - startTime)
        println("Time Taken " + time + " milli seconds ")
        //Thread.sleep(1000)
        _StatsList.add(i + "," + time)
      }
      createStatsFile("C:\\Raw_Data_Source_For_Test\\TestResults\\", batchSize)
      _RecordsList.clear()
      _StatsList.clear()
      println("Batch Size: " + batchSize + " Processing Complete.")
    }
  }


  def createRecordsBatch(dirPath: String, batchSize: Int) {
    val filteredFiles = new File(dirPath).listFiles.filter(_.getName.matches("[0-9]{1,5}_(.*?)_(.*?)_ALLOUT(.*?).CSV"))
    val _batch = new ArrayList[String]()
    var count = 0
    filteredFiles.foreach {
      file =>
        //println("Processing: " + file.getName)
        val sc = new Scanner(file)
        sc.nextLine() // skip header
        while (sc.hasNextLine()) {
          val arrayOfElements = sc.nextLine().split(',')
          if (_batch.size() == batchSize) {
            _RecordsList.add(_batch.clone().asInstanceOf[ArrayList[String]])
            //            val printWriter = new PrintWriter(dirPath + "\\" + batchSize + "\\" + count + ".json")
            //            _batch.foreach { record => printWriter.println(record) }
            //            printWriter.close()
            _batch.clear()
            count = count + 1
          }
          _batch.add(prepareJSONObject(arrayOfElements))
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
    val geocode = GeoHash.encode(arrayOfElemets(22).toDouble, arrayOfElemets(23).toDouble, 8)
    jSONObject.put("geocode", geocode)
    jSONObject.toString()
  }

  def createStatsFile(dirPath: String, batchSize: Int): Unit = {

    val printWriter = new PrintWriter(dirPath + batchSize + ".csv")
    printWriter.println("Batch No., Time Taken(milliSec)")
    _StatsList.foreach {
      line =>
        printWriter.println(line)
    }
    printWriter.close()
  }


}
