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



object ListAccumulator3 extends AccumulatorParam[List[(Long, HashSet[String])]] {

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
/*
 *Question 3:
 *Find the average number of words in every neighbor of a vertex. Hint: to solve this question please refer
 * to the Neighborhood Aggregation examples from here
 */

object partB_App2_q3 {

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

    val sc = new SparkContext(conf)

    //Get the configuration of the file system to accesss
    val fs_conf = new Configuration()
    val fs = FileSystem.get(fs_conf)


    //Open the directory of timeline files
    val path = new Path("storm_output_words")
    //    val path = new Path("/Users/mushahidalam/workspace/GraphX/data/tmp")

    //--------------------------------Graph Generation Begins---------------------------------------------
    //Get the directory iterator
    val directory_iterator = fs.listFiles(path, false)

    //Create an accumulator which contains array of List[(Long, HashSet[String])]
    var verticesContentAcc = sc.accumulator(ListAccumulator.zero(Nil))(ListAccumulator)

    var i = 0L //Vertex Indicies
    while (directory_iterator.hasNext()) {
      //Get the path of the file
      var path = directory_iterator.next().getPath().toString

      //Create an accumulator which stores words of the file in HashSet
      val AccumulatorFileContentsToHashSet = sc.accumulableCollection(HashSet[String]())

      //Open the textFile in SparkContext
      var TimeLineFile = sc.textFile(path)

      //Fetch each word and insert into the hashSet Accumulator
      TimeLineFile.foreach( x => AccumulatorFileContentsToHashSet += x.toString)

      //Insert the HashSet into the Vertices content Accumulator
      val elem  = List[(Long, HashSet[String])]((i, AccumulatorFileContentsToHashSet.value))
      verticesContentAcc.add(elem)

      //Increment the Vertex index
      i += 1L
    }

    //Create a Vertices RDD from the Accumulator of List[Long,HashSet[String])]
    val verticesRDD = sc.parallelize(verticesContentAcc.value)

    //Do a cartesian to check if a vertex has common words with any other vertex
    val verticesCartesianOutput = verticesRDD.cartesian(verticesRDD)

    //Do a HashSet Intersect to get if there are atleast one common words
    val vertexPairAtleastOneCommonWord = verticesCartesianOutput.map { case ((a: Long, b: HashSet[String]), (c: Long, d: HashSet[String]))
    => (a, c, Math.min(b.intersect(d).size, 1))
    }

    //Filter entries which doesn't have atleast one common word
    val filteredEdgeRDD = vertexPairAtleastOneCommonWord.filter{ case (a: Long, b: Long, c: Int) => c != 0 && a!=b}

    //Change the Edge Attribute to 0 and change the triplet pair of filteredRDD to Edge type
    val EdgeRDD = filteredEdgeRDD.map{case (a: Long,b:Long, c: Int)=> Edge(a,b, 0)}

    //--------------------------------Graph Generation Ends---------------------------------------------
    //Create the graph from verticesRDD and EdgeRDD
    val graph: Graph[HashSet[String], Int] = Graph(verticesRDD, EdgeRDD)


    //Transform graph such that vertex contains the outdegree
    val DegreeGraph: Graph[(HashSet[String],Int), Int] =
      graph.outerJoinVertices(graph.outDegrees){case(vid, b:HashSet[String], degOpt) => (b, degOpt.getOrElse(0))}

    //Send function for the AggregateMessage Api which sends neighbour vertex words count.
    def SendNeighbourAtrrSizeToSource(triplet: EdgeContext[(HashSet[String],Int), Int, Int]) {
      triplet.sendToSrc(triplet.dstAttr._1.size)
    }
    //Reduce function for the AggregateMessage Api
    def AccumulateNeighbourAttrSize(a: Int, b: Int): Int = a + b

    //Collect the neighbour words count sum using AggregateMessage Api
    val result = DegreeGraph.aggregateMessages[Int](SendNeighbourAtrrSizeToSource, AccumulateNeighbourAttrSize)

    //Divide the neighbour words count sum/Outdegree = average number of words in every neighbor of a vertex
    val graphWithNeighbourAverage:Graph[Float, Int] = DegreeGraph.outerJoinVertices(result)({case(vid:VertexId,vertexAtr:(HashSet[String],Int),total:Option[Int]) => if (vertexAtr._2 >0)  total.get.asInstanceOf[Int]/vertexAtr._2 else 0})

    //Remove vertices with outdegree Zero
    val filteredResultGraph = graphWithNeighbourAverage.vertices.filter{case(a:VertexId,b:Float) => b != 0}

    filteredResultGraph.saveAsTextFile("/home/ubuntu/output/App2Question3_output.txt")

    println("==============================================")
    println("Application2 Question 3: average number of words in every neighbor of a vertex")
    filteredResultGraph.foreach(println)
    println("==============================================")

    sc.stop()

  }
}

