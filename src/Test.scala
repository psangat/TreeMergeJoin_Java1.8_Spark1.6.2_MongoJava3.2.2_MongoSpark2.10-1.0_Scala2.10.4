
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.bson.Document

/**
  * Created by psangats on 11/07/2016.
  */
object Test {
  def main(args: Array[String]) {
    // issue#1: java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
    System.setProperty("hadoop.home.dir", "C:\\Applications\\")
    // end issue#1
    val conf = new SparkConf().setAppName("TreeMerge").setMaster("local[*]")
    /*conf.set("spark.mongodb.input.uri", "mongodb://127.0.0.1")
    conf.set("spark.mongodb.input.database", "test")
    conf.set("spark.mongodb.input.collection", "collectionCars")
    conf.set("spark.mongodb.input.readPreference.name", "primaryPreferred")*/
    //conf.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.collectionTracks")
    val sc = new SparkContext(conf)
    val readConfigCars = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/test.collectionCars"))
    val readConfigTracks = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/test.collectionTracks"))

    val collectionCars = MongoSpark.load(sc, readConfigCars) //sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
    val collectionTracks = MongoSpark.load(sc, readConfigTracks)
    val bvCollectionTracks = sc.broadcast(collectionTracks)
    println("Number of partitions(Collection Cars): " + collectionCars.getNumPartitions)
    val partitionedCollectionCars = collectionCars.repartition(3)
    println("Number of partitions(Collection Cars) after partitioning: " + partitionedCollectionCars.getNumPartitions)

    // Distributed Nested Loop Join
    // Using Round Robin Partitioning
    partitionedCollectionCars.foreachPartition {
      part =>
        val tc = TaskContext.get
        val partId = tc.partitionId() + 1
        part.foreach { docCars =>
          bvCollectionTracks.value.foreach { docTracks =>
            if (docCars.get("geocode").equals(docTracks.get("geocode"))) {
              val mergeDoc = new Document()
              mergeDoc.put("cars", docCars)
              mergeDoc.put("tracks", docTracks)
              mergeDoc.put("partitionID", partId)
              println(mergeDoc)
            }
          }
        }
    }
    //MongoSpark.save(documents)
    sc.stop()
  }
}
