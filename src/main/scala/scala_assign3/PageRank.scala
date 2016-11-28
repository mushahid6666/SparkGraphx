package scala_assign3

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

import scala.io.Source
import scala.reflect.ClassTag

/**
 * @author ${mushahid.alam}
 */
object PageRank {

  def main(args : Array[String]): Unit = {

    val conf = new SparkConf()
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


    val sc = new SparkContext(conf)

    val graph= GraphLoader.edgeListFile(sc, "data/web-BerkStan.txt", true).partitionBy(PartitionStrategy. RandomVertexCut)
    //val graph= GraphLoader.edgeListFile(sc, "data/soc-LiveJournal1.txt", true)



    val src: VertexId = -1L

    var rankGraph:Graph[Double, Double] = graph
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
      .mapVertices{(id, attr) => 1.0}


    var i = 0

    var resetProb: Double = 0.15


    var prevRankGraph: Graph[Double, Double] = null


    while (i < 20) {
      rankGraph.cache()

      val rankUpdates = rankGraph.aggregateMessages[Double](
      ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

      prevRankGraph = rankGraph

      val rPrb = (src: VertexId, id: VertexId) => resetProb

      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => rPrb(src, id) + (1.0 - resetProb) * msgSum
      }.cache()


      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices

      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      i += 1
    }

    rankGraph.vertices.saveAsTextFile("hdfs:/home/ubuntu/output")

    sc.stop()
  }

}
