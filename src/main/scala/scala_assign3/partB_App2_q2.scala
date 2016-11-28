package scala_assign3


import java.io.{PrintWriter, FileWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.HashSet
import scala.io.Source
import java.net.URL
import java.io._

import org.apache.spark.{AccumulatorParam, SparkContext, SparkConf}

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.SparkSession
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
/**
 * Created by mushahidalam on 11/26/16.
 */



object ListAccumulator2 extends AccumulatorParam[List[(Long, HashSet[String])]] {

  val vertex_property =  List[(Long,HashSet[String])]()
  def zero(initialValue: List[(Long, HashSet[String])]): List[(Long, HashSet[String])] = {
    Nil
  }

  def addInPlace(list1: List[(Long, HashSet[String])], list2: List[(Long, HashSet[String])]): List[(Long, HashSet[String])] = {
    list1 ::: list2
  }

  def add(ele:(Long, HashSet[String])): List[(Long, HashSet[String])] = {
    vertex_property:+ ele
  }
}



object partB_app2_q2 {

  def main(args : Array[String]): Unit = {


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

    var i = 0L

    val sc = new SparkContext(conf)

    //Set the configuration of the file system
    val fs_conf = new Configuration()
    //    fs_conf.set("fs.defaultFS", "hdfs://10.254.0.53:8020")
    val fs = FileSystem.get(fs_conf)


    //Open the directory of timeline files
    val path = new Path("storm_output_words")
//    val path = new Path("/Users/mushahidalam/workspace/GraphX/data/q2_data")
    val file1_iterator = fs.listFiles(path, false)

    //    var vertexArray: Array[Edge[Int]] = new Array[Edge[Int]](0)

    var vertexArray = sc.accumulator(ListAccumulator2.zero(Nil))(ListAccumulator2)

    while (file1_iterator.hasNext()) {
      var path = file1_iterator.next().getPath().toString

      val AccumulatorFileContentsToHashSet = sc.accumulableCollection(HashSet[String]())

      var TimeLineFile = sc.textFile(path)

      TimeLineFile.foreach( x => AccumulatorFileContentsToHashSet += x.toString)

      val elem  = List[(Long, HashSet[String])]((i, AccumulatorFileContentsToHashSet.value))
      vertexArray.add(elem)

      i += 1L
    }

    val vertexRDD = sc.parallelize(vertexArray.value)

    val verticescartesianoutput = vertexRDD.cartesian(vertexRDD)

    val vertexPairAtleastOneCommonWord = verticescartesianoutput.map { case ((a: Long, b: HashSet[String]), (c: Long, d: HashSet[String]))
    => (a, c, Math.min(b.intersect(d).size, 1))
    }

    val filteredEdgeRDD = vertexPairAtleastOneCommonWord.filter{ case (a: Long, b: Long, c: Int) => c != 0 && a!=b}

    val EdgeRDD = filteredEdgeRDD.map{case (a: Long,b:Long, c: Int)=> Edge(a,b, 0)}


    val graph: Graph[HashSet[String], Int] = Graph(vertexRDD, EdgeRDD)


    // Define a reduce operation to compute the highest degree vertex
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }


    //Compute the max outdegree vertices
    val outDegreeRDD = graph.outDegrees
    val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)

    val outDegreeSet = outDegreeRDD.filter( x => x._2 == maxOutDegree._2)


//    println("Max outdegree verticess")
//    outDegreeSet.foreach(println)
//    println("===============")
    if(outDegreeSet.count > 0){
      //Incase we have X=1, we stop. If X > 1, we use the next condition as a tie-breaker - among the X vertices,
      // choose the one which has the maximum number of words (in that vertex or interval). Let's say we now have
      // a set of Y vertices.
      val verticesMaxOutdegree = graph.vertices.join(outDegreeSet)

//      println("Joing Output")
//      verticesMaxOutdegree.foreach(println)
      def maxWords(a: (VertexId, (HashSet[String],Int)), b: (VertexId, (HashSet[String],Int))): (VertexId, (HashSet[String],Int)) = {
        if (a._2._1.size > b._2._1.size) a else b
      }

      val FinalVertices:(VertexId, (HashSet[String],Int)) = graph.vertices.join(outDegreeSet).reduce(maxWords)


      //Final Vertices with max words and outdegree

      val maxWordsVertices = verticesMaxOutdegree.filter(x => x._2._1.size == FinalVertices._2._1.size)

      println(maxWordsVertices.values.first()._2)
      val output_path = new Path("/home/ubuntu/output/q2_output.txt")
//      val output_path = new Path("/Users/mushahidalam/workspace/GraphX/data/output/q2_output.txt")

      val output_file = fs.create(output_path)
      output_file.writeByte(maxWordsVertices.values.first()._2)
      output_file.close()

      //      println("Size" +FinalVertices._2._1.size)
//      maxWordsVertices.foreach(println)


      //Filter on Max words
    }


    sc.stop()

  }

}
