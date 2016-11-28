import java.io.File
import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by psangats on 21/10/2016.
  */
object DataSendingSimulator {
  def main(args: Array[String]) {
    //fileCopySimulation("C:\\Raw_Data_Source_For_Test\\AllFiles\\Converted\\10000")
    //"/mnt/TestData/10000/"
    fileCopySimulation(args(0))
  }

  def fileCopySimulation(directoryPath: String): Unit = {
    val hadoopconf = new Configuration
    val hdfsURI = "hdfs://localhost:9000"
    hadoopconf.set("fs.defaultFS", hdfsURI)
    val hdfs = FileSystem.get(hadoopconf)
    val filteredFiles = new File(directoryPath).listFiles()
    filteredFiles.foreach {
      file =>
        val srcPath = new Path(file.getAbsolutePath())
        val destPath = new Path("hdfs://localhost:9000/inputDirectory/" + file.getName())
        hdfs.copyFromLocalFile(false, srcPath, destPath)
        println("[Directory] : " + file.getName + " Copied\n Going to sleep ...")
        TimeUnit.SECONDS.sleep(1)
    }
  }
}
