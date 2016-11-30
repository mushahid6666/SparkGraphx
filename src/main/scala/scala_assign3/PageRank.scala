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

    val graph= GraphLoader.edgeListFile(sc, "data/soc-LiveJournal1.txt", true).partitionBy(PartitionStrategy. RandomVertexCut)

    val src: VertexId = -1L

    var rankGraph:Graph[Double, Double] = graph
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }//Each vertex now contains num of neighbours(outdegree) as attr
      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)//Edge Contains 1.0/|neighbors(p)|
      .mapVertices{(id, attr) => 1.0} // Vertices contain intial rank as 1.0


    var i = 0

    var prevRankGraph: Graph[Double, Double] = null


    while (i < 20) {
      rankGraph.cache()

      //Send to each vertex (vertex.Attrx(Rank) * Edge.attr(1.0/neighbour) = rank(p)/|neighbors(p)|
      //Aggregate the received contributions from neighbours at each vertex _+_
      //And TripletFields.Src is used to notify GraphX that only src part of the EdgeContext will
      // be needed allowing GraphX to select an optimized join strategy
      val Contributions = rankGraph.aggregateMessages[Double](
      triplets => triplets.sendToDst(triplets.srcAttr * triplets.attr), _ + _, TripletFields.Src)

      prevRankGraph = rankGraph


      //Update the rank of each vertex by doing a join of prevrank graph and Contributions recieved from
      //neighbouring vertices and set each page's rank to 0.15 + 0.85 X contributions.
      rankGraph = rankGraph.joinVertices(Contributions) {
        (id, oldRank, contrib) => 0.15 + 0.85 * contrib
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
