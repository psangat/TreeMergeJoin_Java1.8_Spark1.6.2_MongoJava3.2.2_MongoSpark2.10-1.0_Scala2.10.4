import java.io.{File, PrintWriter}
import java.util.{ArrayList, Scanner}

import scala.collection.JavaConversions._


/**
  * Created by psangats on 21/10/2016.
  */
object TestDataCreator {
  val _RecordsList = new ArrayList[String]()
  val _StatsList = new ArrayList[String]()

  def main(args: Array[String]) {
    val startTimeBatch = System.currentTimeMillis()
    createRecordsBatch("C:\\\\Raw_Data_Source_For_Test\\\\AllFiles", 20000)
    val endTimeBatch = System.currentTimeMillis()
    println("Time Taken (Batchs Creation): " + (endTimeBatch - startTimeBatch) + " milli seconds ")

  }

  def createRecordsBatch(dirPath: String, batchSize: Int) {
    val filteredFiles = new File(dirPath).listFiles.filter(_.getName.matches("[0-9]{1,4}_(.*?)_(.*?)_ALLOUT(.*?).CSV"))
    val _Batch = new ArrayList[String]()
    var count = 0
    val outputDir = dirPath + "\\Converted\\" + batchSize + "\\"
    val directory = new File(String.valueOf(outputDir))
    if (!directory.exists()) {
      directory.mkdir()
    }
    filteredFiles.foreach {
      file =>
        //println("Processing: " + file.getName)
        val sc = new Scanner(file)
        sc.nextLine() // skip header
        while (sc.hasNextLine()) {
          val values = sc.nextLine().split(',')
          _RecordsList.add("{\"iSegment\":%s,\"SomatTime\":%s,\"kmh\":%s,\"CarOrient\":%s,\"EorL\":%s,\"minSND\":%s,\"maxSND\":%s,\"Rock\":%s,\"Bounce\":%s,\"minCFA\":%s,\"maxCFA\":%s,\"accR3\":%s,\"accR4\":%s,\"Direction\":\"%s\",\"minCFB\":%s,\"maxCFB\":%s,\"LATACC\":%s,\"maxBounce\":%s,\"PipeA\":%s,\"PipeB\":%s,\"latitude\":%s,\"longitude\":%s}"
            .format(values(0), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(15), values(16), values(17), values(18).replace(".", ""), values(19), values(20), values(21), values(22), values(23)))
          if (_RecordsList.size() >= batchSize) {
            count = count + 1
            printFile(outputDir, count)
          }
        }
        sc.close()
    }
  }

  def printFile(saveDir: String, count: Int): Unit = {
    //
    //val pw = new PrintWriter(new File("/mnt/outputFiles/SampleFile" + count + ".json"))
    val pw = new PrintWriter(new File(saveDir + count + ".json"))
    _RecordsList.foreach { record =>
      pw.println(record)
    }
    pw.close
    _RecordsList.clear()
    //System.exit(1)
  }
}
