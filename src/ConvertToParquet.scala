import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by psangats on 3/08/2016.
  */
object ConvertToParquet {

  def main(args: Array[String]) {
    // issue#1: java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
    System.setProperty("hadoop.home.dir", "C:\\Applications\\")
    // end issue#1
    val conf = new SparkConf().setAppName("Convert to parquet").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqc = new SQLContext(sc)

        val dfEnvironment = sqc.read.format("json").load("C:\\Raw_Data_Source_For_Test\\AllFiles\\ConvertedToJSON_EnvironmentSensors_ForParquet.json")
        dfEnvironment.write.parquet("C:\\Raw_Data_Source_For_Test\\AllFiles\\\\EnvironmentSensors.parquet")
        val dfCars = sqc.read.format("json").load("C:\\Raw_Data_Source_For_Test\\AllFiles\\ConvertedToJSON_CarSensors_ForParquet.json")
        dfCars.write.parquet("C:\\Raw_Data_Source_For_Test\\AllFiles\\\\CarSensors.parquet")
  }

}
