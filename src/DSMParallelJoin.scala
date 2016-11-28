

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SizeEstimator

import scala.reflect.ClassTag

/**
  * Created by psangats on 11/11/2016.
  */
class CustomRangePartitioner(numParts: Int) extends Partitioner {
  require(numParts >= 0, s"Number of partitions ($numParts) cannot be negative.")

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case x if 11 until 20 contains x => 0 // (key.hashCode % numPartitions)
    case y if 21 until 30 contains y => 1
    case z if 31 until 40 contains z => 2
  }

  // Java equals method to let Spark compare our Partitioner objects
  override def equals(other: Any): Boolean = other match {
    case h: CustomRangePartitioner =>
      (h.numPartitions == numPartitions)
    case _ =>
      false
  }

  override def numPartitions: Int = numParts
}

object DSMParallelJoin {
  val names_R = new Array[String](36)
  names_R(11) = "Adele"
  names_R(12) = "Bob"
  names_R(13) = "Clement"
  names_R(14) = "Dave"
  names_R(15) = "Ed"
  names_R(21) = "Fung"
  names_R(22) = "Goel"
  names_R(23) = "Harry"
  names_R(24) = "Irene"
  names_R(25) = "Joanna"
  names_R(31) = "Kelly"
  names_R(32) = "Lim"
  names_R(33) = "Meng"
  names_R(34) = "Noor"
  names_R(35) = "Omar"

  val codes_R = new Array[(Int, Int)](36)
  codes_R(11) = (11, 8)
  codes_R(12) = (12, 22)
  codes_R(13) = (13, 16)
  codes_R(14) = (14, 23)
  codes_R(15) = (15, 11)
  codes_R(21) = (11, 25)
  codes_R(22) = (22, 3)
  codes_R(23) = (23, 17)
  codes_R(24) = (24, 14)
  codes_R(25) = (25, 2)
  codes_R(31) = (31, 6)
  codes_R(32) = (32, 20)
  codes_R(33) = (33, 1)
  codes_R(34) = (34, 5)
  codes_R(35) = (35, 19)

  val address_R = new Array[String](36)
  address_R(11) = "Clayton"
  address_R(12) = "Caulfield"
  address_R(13) = "Caulfield"
  address_R(14) = "Caulfield"
  address_R(15) = "Kew"
  address_R(21) = "Caulfield"
  address_R(22) = "Caulfield"
  address_R(23) = "Caulfield"
  address_R(24) = "Clayton"
  address_R(25) = "Clayton"
  address_R(31) = "Clayton"
  address_R(32) = "Caulfield"
  address_R(33) = "Caulfield"
  address_R(34) = "Caulfield"
  address_R(35) = "Kew"


  val faculty_S = new Array[String](36)
  faculty_S(11) = "Arts"
  faculty_S(12) = "Dance"
  faculty_S(13) = "Geology"
  faculty_S(21) = "Business"
  faculty_S(22) = "Engineering"
  faculty_S(23) = "Health"
  faculty_S(31) = "CompSc"
  faculty_S(32) = "Finance"
  faculty_S(33) = "IT"

  val codes_S = new Array[(Int, Int)](36)
  codes_S(11) = (11, 8)
  codes_S(12) = (12, 15)
  codes_S(13) = (13, 10)
  codes_S(21) = (11, 12)
  codes_S(22) = (22, 7)
  codes_S(23) = (23, 11)
  codes_S(31) = (31, 2)
  codes_S(32) = (32, 21)
  codes_S(33) = (33, 18)

  val names_RO = List(11 -> "Adele",
    12 -> "Bob",
    13 -> "Clement",
    14 -> "Dave",
    15 -> "Ed",
    21 -> "Fung",
    22 -> "Goel",
    23 -> "Harry",
    24 -> "Irene",
    25 -> "Joanna",
    31 -> "Kelly",
    32 -> "Lim",
    33 -> "Meng",
    34 -> "Noor",
    35 -> "Omar")
  val codes_RO = List(11 -> 8,
    12 -> 22,
    13 -> 16,
    14 -> 23,
    15 -> 11,
    21 -> 25,
    22 -> 3,
    23 -> 17,
    24 -> 14,
    25 -> 2,
    31 -> 6,
    32 -> 20,
    33 -> 1,
    34 -> 5,
    35 -> 19)
  val address_RO = List(11 -> "Clayton",
    12 -> "Caulfield",
    13 -> "Caulfield",
    14 -> "Caulfield",
    15 -> "Kew",
    21 -> "Caulfield",
    22 -> "Caulfield",
    23 -> "Caulfield",
    24 -> "Clayton",
    25 -> "Clayton",
    31 -> "Clayton",
    32 -> "Caulfield",
    33 -> "Caulfield",
    34 -> "Caulfield",
    35 -> "Kew")
  val faculty_SO = List(11 -> "Arts",
    12 -> "Dance",
    13 -> "Geology",
    21 -> "Business",
    22 -> "Engineering",
    23 -> "Health",
    31 -> "CompSc",
    32 -> "Finance",
    33 -> "IT")
  val codes_SO = List(11 -> 8,
    12 -> 15,
    13 -> 10,
    21 -> 12,
    22 -> 7,
    23 -> 11,
    31 -> 2,
    32 -> 21,
    33 -> 18)


  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // issue#1: java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
    System.setProperty("hadoop.home.dir", "C:\\Applications\\")
    // end issue#1
    val conf = new SparkConf().setAppName("DSM Parallel Join")
      .setMaster("local[3]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    // Original Data Placement
    val namesR = sc.parallelize(names_R, 3)
    val addressR = sc.parallelize(address_R, 3)
    val codesR = sc.parallelize(codes_RO, 3).partitionBy(new CustomRangePartitioner(3))
    val facultyS = sc.parallelize(faculty_S, 3)
    val codesS = sc.parallelize(codes_SO, 3).partitionBy(new CustomRangePartitioner(3))
    // printRddCollection(namesR)
    // printRddCollection(codesR)
    // End Original Data Placement

    println("Algorithm 1")
    println("==============")
    val sLTime = System.currentTimeMillis()
    // Distribution Phase (based on Join Attribute)
    val hashTableR = distributeColumnUsingHash(codesR.map(_.swap)) // y is the join attribute
    // printRddCollection(hashTableR)
    val hashTableS = distributeColumnUsingHash(codesS.map(_.swap)) // y is the join attribute
    // printRddCollection(hashTableS)
    // End Distribution Phase

    // Local Join on Hash Tables
    // For Optimizations: https://gist.github.com/samklr/1cc431c130f145c24d3685a9b59a0fea
    val localJoinResult = hashTableR.join(hashTableS)
    // printRddCollection(localJoinResult)
    // End Local Join on Hash Tables

    // Redistribution of Join Result (based on range(rowID))
    // val newRDD = listRDD.flatMap{case(x, y, z) => z.map((x,y,_))}
    val mappedResultR = localJoinResult.map(row => (row._2._1, row._1))
    // printRddCollection(mappedResultR)
    val mappedResultS = localJoinResult.map(row => (row._2._2, row._1))
    // println("=============================")
    // printRddCollection(mappedResultS)
    // println("=============================")
    val redistributeDataR = mappedResultR.partitionBy(new CustomRangePartitioner(3))
    //val redistributeDataS = mappedResultS.partitionBy(new RangePartitioner(3, mappedResultS))
    val redistributeDataS = mappedResultS.partitionBy(new CustomRangePartitioner(3))

    // printRddCollection(redistributeDataR)
    // println("=============================")
    // printRddCollection(redistributeDataS)
    // End Redistribution of Join Result

    // Tuple Reconstruction
    val mappedR = redistributeDataR.map(item => (item._2, (names_R.apply(item._1), address_R.apply(item._1))))
    // mappedR.foreach(println)
    //    redistributeDataR  .foreachPartition(part => {
    //      part.foreach(item => {
    //        val rowID = item._1
    //
    //        println(item._1 + " , " +item._2 + " , " + names_R.apply(rowID) + " , " + address_R.apply(rowID))
    //      })
    //    })
    val mappedS = redistributeDataS.map(item => (item._2, faculty_S.apply(item._1)))
    // mappedS.foreach(println)
    //    redistributeDataS.foreachPartition(part => {
    //      part.foreach(item => {
    //        val rowID = item._1
    //        println(item._1 + " , " + item._2 + " , " + faculty_S.apply(rowID))
    //      })
    //    })

    // End Tuple Reconstruction

    // Hash Redistribution
    val hashR = distributeColumnUsingHash(mappedR)
    val hashS = distributeColumnUsingHash(mappedS)
    // End Hash Redistribution

    // Final Join
    val finalResult = hashR.join(hashS)
    finalResult.foreach(println)
    val eLTime = System.currentTimeMillis()
    val agl1RunTime = (eLTime - sLTime)
    println("Algorithm 1 Running Time:    " + agl1RunTime + "ms")
    // End Final Join


    println("Algorithm 2")
    println("==============")
    val sLTime2 = System.currentTimeMillis()

    // Tuple Reconstruction
    val mappedR2 = codesR.map(item => (item._2, (names_R.apply(item._1), address_R.apply(item._1))))
    // mappedR.foreach(println)
    //    redistributeDataR  .foreachPartition(part => {
    //      part.foreach(item => {
    //        val rowID = item._1
    //
    //        println(item._1 + " , " +item._2 + " , " + names_R.apply(rowID) + " , " + address_R.apply(rowID))
    //      })
    //    })
    val mappedS2 = codesS.map(item => (item._2, faculty_S.apply(item._1)))
    // mappedS.foreach(println)
    //    redistributeDataS.foreachPartition(part => {
    //      part.foreach(item => {
    //        val rowID = item._1
    //        println(item._1 + " , " + item._2 + " , " + faculty_S.apply(rowID))
    //      })
    //    })

    // End Tuple Reconstruction

    // Hash Redistribution
    val hashR2 = distributeColumnUsingHash(mappedR2)
    val hashS2 = distributeColumnUsingHash(mappedS2)
    // End Hash Redistribution

    // Local Join (on Join Attribute)
    val finalResult2 = hashR2.join(hashS2)
    finalResult2.foreach(println)
    val eLTime2 = System.currentTimeMillis()
    val agl1RunTime2 = (eLTime2 - sLTime2)
    println("Algorithm 2 Running Time:    " + agl1RunTime2 + "ms")
    // End Final Join
  }

  def distributeColumnUsingHash[T: ClassTag](column: RDD[(Int, T)]) = column.partitionBy(new HashPartitioner(3))

  def getTotalSize[T](rdd: RDD[T]): Long = {
    // This can be a parameter
    val NO_OF_SAMPLE_ROWS = 10l;
    val totalRows = rdd.count();
    var totalSize = 0l
    if (totalRows > NO_OF_SAMPLE_ROWS) {
      val sampleRDD = rdd.sample(true, NO_OF_SAMPLE_ROWS)
      val sampleRDDSize = getRDDSize(sampleRDD)
      totalSize = sampleRDDSize.*(totalRows)./(NO_OF_SAMPLE_ROWS)
    } else {
      // As the RDD is smaller than sample rows count, we can just calculate the total RDD size
      totalSize = getRDDSize(rdd)
    }
    totalSize
  }

  def getRDDSize[T](rdd: RDD[T]): Long = {
    var rddSize = 0l
    val rows = rdd.collect()
    for (i <- 0 until rows.length) {
      rddSize += SizeEstimator.estimate(Seq(rows.apply(i)).map { value => value.asInstanceOf[AnyRef] })
    }
    rddSize
  }

  def countByPartition[T](rdd: RDD[T]) = {
    rdd.mapPartitions(iter => {
      val tc = TaskContext.get
      Iterator(("Partition #" + tc.partitionId(), "Item Count: " + iter.length))
    })
  }

  def printRddCollection[T](collections: RDD[T]) = {
    val tc = TaskContext.get
    collections.foreachPartition(part => {
      val tc = TaskContext.get
      part.foreach(x => println("Partition #:" + tc.partitionId() + "    Values : " + x))
    })
  }


}
