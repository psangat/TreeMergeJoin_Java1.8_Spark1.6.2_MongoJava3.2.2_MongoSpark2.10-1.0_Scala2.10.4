import java.util

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

/**
  * Created by psangats on 3/08/2016.
  */
object JoinsOnParquet {
  val colA = new util.HashMap[Int, String]()
  val colB = new util.HashMap[Int, String]()
  val colC = new util.HashMap[Int, String]()

  val colX = new util.HashMap[Int, String]()
  val colY = new util.HashMap[Int, String]()
  val colZ = new util.HashMap[Int, String]()

  def main(args: Array[String]): Unit = {
    SPARKSQLJoinsOnParquet()
    //ColumnJoinsOnParquet()
    //visualtizationDataLayout()
    //storageDataLayout()
  }

  def test(): Unit = {

  }

  def visualtizationDataLayout(): Unit = {
    for (i <- 1 to 10) {
      colA.put(i, i.toString)
      colB.put(i, RawToMongo.getRandomTemperature(1, 10).toString)
      colC.put(i, RawToMongo.getRamdomHumidity())

      colX.put(i, i.toString)
      colY.put(i, RawToMongo.getRandomTemperature(5, 8).toString)
      colZ.put(i, RawToMongo.getRamdomHumidity())
    }
  }

  def storageDataLayout(): Unit = {
    // issue#1: java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
    System.setProperty("hadoop.home.dir", "C:\\Applications\\")
    // end issue#1
    val conf = new SparkConf().setAppName("Convert to parquet").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val _colA = sc.parallelize(colA.toSeq).map { col => col._2 -> col._1 }
    //_colA.foreachPartition(part=> part.foreach(println))//.reduceByKeyLocally { case (k, v) => k + ":" + v }

    val _colB = sc.parallelize(colB.toSeq).map(col => col._2 -> col._1).groupByKey()
    println("Count COlB : " + _colB.count)


    val _colY = sc.parallelize(colY.toSeq).map(col => col._2 -> col._1).groupByKey()
    println("Count COlY : " + _colY.count)

    val _intersectedData = _colB.join(_colY)
    _intersectedData.foreachPartition(part => part.foreach(row =>
      row._2._1.foreach(idx1 =>
        row._2._2.foreach(idx2 => println(colA.get(idx1)+ ":" + colB.get(idx1) + ":" + colC.get(idx1) + ":" + colX.get(idx2) + ":" + colY.get(idx2) + ":" + colZ.get(idx2)))
      )))
  }


  def ColumnJoinsOnParquet(): Unit = {

    // issue#1: java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
    System.setProperty("hadoop.home.dir", "C:\\Applications\\")
    // end issue#1
    val conf = new SparkConf().setAppName("Convert to parquet").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqc = new SQLContext(sc)
    val carData = sqc.read.parquet("C:\\Raw_Data_Source_For_Test\\AllFiles\\CarSensors.parquet")

    val envData = sqc.read.parquet("C:\\Raw_Data_Source_For_Test\\AllFiles\\EnvironmentSensors.parquet")


    val longs = carData.col("lon")

    println(carData.join(envData, Seq("lat", "lon")).count()) //.show()

  }

  def SPARKSQLJoinsOnParquet(): Unit = {
    // issue#1: java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
    System.setProperty("hadoop.home.dir", "C:\\Applications\\")
    // end issue#1
    val conf = new SparkConf().setAppName("Convert to parquet").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqc = new SQLContext(sc)

    val envData = sqc.read.parquet("C:\\Raw_Data_Source_For_Test\\AllFiles\\EnvironmentSensors.parquet")
    envData.registerTempTable("envDataTable")
    envData.columns.foreach(println)
    val carData = sqc.read.parquet("C:\\Raw_Data_Source_For_Test\\AllFiles\\CarSensors.parquet")
    carData.registerTempTable("carDataTable")
    carData.columns.foreach(println)

    val startTime = System.currentTimeMillis()
    val joinResult = sqc.sql("SELECT * from envDataTable e,carDataTable c WHERE e.lat = c.lat and e.lon = c.lon").toDF().show()
    val endtTime = System.currentTimeMillis()

    println("Time Taken " + ((endtTime - startTime) / 1000) + " seconds")
  }
}
