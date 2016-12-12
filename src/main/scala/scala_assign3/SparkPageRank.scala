package scala_assign3

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.Partitioner;

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 */
class CustomPartitioner extends Partitioner {

  override def numPartitions: Int = 20

  override def getPartition(key: Any):Int = {
    var v=  key.asInstanceOf[String]
    return Integer.parseInt(v) % this.numPartitions
  }
}

object SparkPageRank {

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
        |Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }

    showWarning()

    val conf = new SparkConf()
//      .setMaster("local[1]")
//      .setAppName("App")
          .setMaster("spark://10.254.0.53:7077")
      .setAppName("App")
      .set("spark.driver.memory", "8g")
      .set("spark.driver.cores", "2")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "hdfs:/tmp/spark-events")
      .set("spark.executor.memory", "4g")
      .set("spark.executor.cores", "1")
      .set("spark.executor.instances","20")
      .set("spark.task.cpus", "1")

    val spark = SparkSession
      .builder
      .appName("SparkPageRank")
      .config(conf)
      .getOrCreate()

    val iters = if (args.length > 1) args(1).toInt else 10
    val lines = spark.read.textFile(args(0)).rdd
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey(new CustomPartitioner()).persist(StorageLevel.MEMORY_ONLY)
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    ranks.saveAsTextFile("hdfs:/home/ubuntu/output")

    spark.stop()
  }
}
// scalastyle:on println
