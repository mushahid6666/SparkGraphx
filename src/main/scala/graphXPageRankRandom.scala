package scala_assign3

import org.apache.spark.graphx.{TripletFields, Graph, PartitionStrategy, GraphLoader}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by mushahidalam on 12/2/16.
 */
object graphXPageRankRandom {
  def main(args: Array[String]): Unit = {

    System.out.println("Usage: Assign2PageRank <DataSet File> <Number of Iterations")
    val conf = new SparkConf()
//      .setMaster("local[1]")
//      .setAppName("Assign3PageRank")
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

    val graph = GraphLoader.edgeListFile(sc, args(0), false, 20).partitionBy(PartitionStrategy.RandomVertexCut)

    val Iterations = if(args.length > 1) args(1).toInt else 10

    //Each vertex now contains num of neighbours(outdegree) as attr
    var PageRankGraph:Graph[Double, Double] = graph.outerJoinVertices(graph.outDegrees){(vertex_id,vertex_attr, vertex_outdegree) => vertex_outdegree.getOrElse(0)}
                                          .mapTriplets(edge_attr => 1.0 /edge_attr.srcAttr, TripletFields.Src ) //Edge Contains 1.0/|neighbors(p)|
                                          .mapVertices{(vertex_id, vertex_attr) => 1.0} // Vertices contain intial rank as 1.0

    var i:Int = 0

    while(i< Iterations){
      PageRankGraph.cache()
      //Send to each vertex (vertex.Attrx(Rank) * Edge.attr(1.0/neighbour) = rank(p)/|neighbors(p)|
      //Aggregate the received contributions from neighbours at each vertex _+_
      //And TripletFields.Src is used to notify GraphX that only src part of the EdgeContext will
      // be needed allowing GraphX to select an optimized join strategy
      val Contributions = PageRankGraph.aggregateMessages[Double](
        Edge_Context => Edge_Context.sendToDst(Edge_Context.srcAttr * Edge_Context.attr), _ + _ , TripletFields.Src)

      //Update the rank of each vertex by doing a join of prevrank graph and Contributions recieved from
      //neighbouring vertices and set each page's rank to 0.15 + 0.85 X contributions.
      PageRankGraph = PageRankGraph.joinVertices(Contributions) {
        (vertex_id, old_rank, contributions) => 0.15 + 0.85 * contributions
      }.cache()

      //Increment the iteration number
      i += 1
    }

    //    PageRankGraph.vertices.foreach(println)
    PageRankGraph.vertices.saveAsTextFile("hdfs:/home/ubuntu/output")
    sc.stop()

  }
}
