import java.util.ArrayList

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by psangats on 26/10/2016.
  */
object RetrieveTest {
  val _StatsList = new ArrayList[String]()
  val iter = 2

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // issue#1: java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
    System.setProperty("hadoop.home.dir", "C:\\Applications\\")
    // end issue#1

    //val conf = new SparkConf().setAppName("Insert Into Mongo Shrads Test")
    // .setMaster("local[1]")
    // .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //val sc = new SparkContext(conf)
    //val sqc = new SQLContext(sc)
    //val readConf = ReadConfig(Map("uri" -> "mongodb://118.138.244.164:27020/CCPE.T1"))
    //val readConf = ReadConfig(Map("uri" -> "mongodb://localhost:27017/agilent.zarkov"))

    println("Start DR_T5 Exact Search")
    for (i <- 1 until iter) {
      //Exact Query Spark
      if (true) {
        val conf = new SparkConf().setAppName("Insert Into Mongo Shrads Test")
          .setMaster("local[4]")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)
        val readConf = ReadConfig(Map("uri" -> "mongodb://118.138.244.164:27020/CCPE.testdata2"))
        val sLTime = System.currentTimeMillis()

        val rdd = MongoSpark.load(sc, readConf)
        val eLTime = System.currentTimeMillis()
        val sFTime = System.currentTimeMillis()
        val filteredRDD = rdd.filter(doc => doc.getString("trackname").equals("MLDD"))
        val eFTime = System.currentTimeMillis()
        val sCTime = System.currentTimeMillis()
        val count = 0 //filteredRDD.count()
        val eCTime = System.currentTimeMillis()
        val loadTime = (eLTime - sLTime)
        val filterTime = (eFTime - sFTime)
        val countTime = (eCTime - sCTime)
        println()
        println("Iteration: " + i + ", Number of records: " + count + " Time Taken :")
        println("   Load:    " + loadTime + "ms")
        println("   Filter:  " + filterTime + "ms")
        println("   Collect: " + countTime + "ms")
        val timeTotal = loadTime + filterTime
        _StatsList.add(i + "," + loadTime + "," + filterTime + "," + countTime + "," + timeTotal)
        sc.stop()

      }
      //End Exact Query Spark
    }

    println("End DR_T5 Exact Search")
    createStatsFile("C:\\Raw_Data_Source_For_Test\\TestResults\\DR_T5.csv")
    println("======================================================")

  }

  def createStatsFile(dirPath: String): Unit = {

    //val printWriter = new PrintWriter(dirPath)
    //printWriter.println("Batch No., Time Taken(milliSec)")
    _StatsList.foreach {
      line =>
        //printWriter.println(line)
        scala.tools.nsc.io.File(dirPath).appendAll(line)
    }
    scala.tools.nsc.io.File(dirPath).appendAll("\r\n")
    //printWriter.close()
  }
}
