import java.io.{File, PrintWriter}

import java.util.{Random, Scanner}

import org.json.JSONObject


/**
  * Created by psangats on 12/07/2016.
  */
object RawToMongo {

  def main(args: Array[String]) {
    createCSVData("C:\\Raw_Data_Source_For_Test\\AllFiles\\")
    /*for (i <- 1 until 10) {
      if (i % 2 == 0) {
        println(i + " True")
      } else println(i + " False")
    }*/
  }

  def convertToJson(dirPath: String) {
    //val jsonObjectsList = new util.ArrayList[JSONObject]()
    //val pattern = Pattern.compile("[0-9]{1,4}_(.*?)_(.*?)_ALLOUT(.*?).CSV")
    val printWriterCarSensor = new PrintWriter(dirPath + "ConvertedToJSON_CarSensors_ForParquet.json")
    val printWriterEnvironmentSensor = new PrintWriter(dirPath + "ConvertedToJSON_EnvironmentSensors_ForParquet.json")
    var count = 0;
    val filteredFiles = new File(dirPath).listFiles.filter(_.getName.matches("[0-9]{1,4}_(.*?)_(.*?)_ALLOUT(.*?).CSV"))
    filteredFiles.foreach {
      file =>
        println("Processing: " + file.getName)
        val sc = new Scanner(file)
        sc.nextLine() // skip header
        while (sc.hasNextLine()) {
          val arrayOfElements = sc.nextLine().split(',')

          printWriterCarSensor.println(prepareJSONObjectCarSensorsForParquet(arrayOfElements))
          count = count + 1
          if (count % 11 == 0) {
            printWriterEnvironmentSensor.println(prepareJSONObjectEnvironmentSensorsForParquet(arrayOfElements))
          }
        }
        sc.close()
        println("Processing: " + file.getName + " Complete.")
    }
    // save file
    printWriterCarSensor.close()
    printWriterEnvironmentSensor.close()
    println("Total Number of Documents: " + count)
    println("Final SAVE completed.")
  }

  def createCSVData(dirPath: String): Unit = {

    val printWriterCarSensor = new PrintWriter(dirPath + "ConvertedToCSV_CarSensors.csv")
    val printWriterEnvironmentSensor = new PrintWriter(dirPath + "ConvertedToCSV_EnvironmentSensors.csv")
    var count = 0;
    val filteredFiles = new File(dirPath).listFiles.filter(_.getName.matches("[0-9]{1,4}_(.*?)_(.*?)_ALLOUT(.*?).CSV"))
    filteredFiles.foreach {
      file =>
        println("Processing: " + file.getName)
        val sc = new Scanner(file)
        sc.nextLine() // skip header
        while (sc.hasNextLine()) {
          val arrayOfElements = sc.nextLine().split(',')
          // header somattime,kmh,carorient,lat,lon,min,max,r3,r4,trackname,direction,trackkm
          printWriterCarSensor.println(prepareCSVDataObjectCarSensors(arrayOfElements))
          count = count + 1
          if (count % 11 == 0) {
            // header somattime,temperature,humidity,lat,lon
            printWriterEnvironmentSensor.println(prepareCSVDataObjectEnvironmentSensors(arrayOfElements))
          }
        }
        sc.close()
        println("Processing: " + file.getName + " Complete.")
    }
    // save file
    printWriterCarSensor.close()
    printWriterEnvironmentSensor.close()
    println("Total Number of Documents: " + count)
    println("Final SAVE completed.")

  }


  def getRamdomHumidity(): String = new Random().nextDouble().toString.substring(0, 4)

  def getRandomTemperature(minimum: Int, maximum: Int): Int = (minimum + new Random().nextInt(maximum - minimum + 1))

  def prepareCSVDataObjectEnvironmentSensors(arrayOfElemets: Array[String]): String = {
    return arrayOfElemets(2) + "," + getRandomTemperature(40, 80) + "," + getRamdomHumidity() + "," + arrayOfElemets(22) + "," + arrayOfElemets(23)

  }

  def prepareCSVDataObjectCarSensors(arrayOfElemets: Array[String]): String = {
    return arrayOfElemets(2) + "," + arrayOfElemets(3) + "," + arrayOfElemets(4) + "," + arrayOfElemets(22) + "," + arrayOfElemets(23) + "," + arrayOfElemets(10) + "," + arrayOfElemets(11) + "," + arrayOfElemets(12) + "," + arrayOfElemets(13) + "," + arrayOfElemets(14) + "," + arrayOfElemets(15) + "," + arrayOfElemets(1)
  }


  def prepareJSONObjectEnvironmentSensorsForParquet(arrayOfElemets: Array[String]): JSONObject = {
    val jSONObject = new JSONObject()
    jSONObject.put("somattime", arrayOfElemets(2))
    jSONObject.put("temperature", getRandomTemperature(40, 80))
    jSONObject.put("humidity", getRamdomHumidity())
    jSONObject.put("lat", arrayOfElemets(22))
    jSONObject.put("lon", arrayOfElemets(23))

  }


  def prepareJSONObjectCarSensorsForParquet(arrayOfElemets: Array[String]): JSONObject = {
    val jSONObject = new JSONObject()
    jSONObject.put("somattime", arrayOfElemets(2))
    jSONObject.put("kmh", arrayOfElemets(3))
    jSONObject.put("carorient", arrayOfElemets(4))
    jSONObject.put("lat", arrayOfElemets(22))
    jSONObject.put("lon", arrayOfElemets(23))

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


  }

  def prepareJSONObjectEnvironmentSensors(arrayOfElemets: Array[String]): JSONObject = {
    val jSONObject = new JSONObject()
    jSONObject.put("somattime", arrayOfElemets(2))
    jSONObject.put("temperature", getRandomTemperature(40, 80))
    jSONObject.put("humidity", getRamdomHumidity())

    val jSONObjectGPS = new JSONObject()
    jSONObjectGPS.put("lat", arrayOfElemets(22))
    jSONObjectGPS.put("lon", arrayOfElemets(23))
    jSONObject.put("gps", jSONObjectGPS)

  }

  def prepareJSONObjectCarSensors(arrayOfElemets: Array[String]): JSONObject = {
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

  }


}
