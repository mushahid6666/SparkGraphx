package scala_assign3

/**
 * Created by mushahidalam on 11/26/16.
 */


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.graphx.{Graph, VertexId, _}
import org.apache.spark.{AccumulatorParam, SparkConf, SparkContext}

import scala.collection.mutable.Map


object ListAccumulator4 extends AccumulatorParam[List[(Long, Map[String,Int])]] {

  val vertex_property = List[(Long,Map[String, Int])]()

  def zero(initialValue: List[(Long, Map[String,Int])]): List[(Long, Map[String,Int])] = {
    Nil
  }

  def addInPlace(list1: List[(Long, Map[String,Int])], list2: List[(Long, Map[String,Int])]): List[(Long, Map[String,Int])] = {
    list1 ::: list2
  }

  def add(ele:(Long, Map[String,Int])): List[(Long, Map[String,Int])] = {
    vertex_property :+ ele
  }
}

object MapAccumulator extends AccumulatorParam[Map[String,Int]] {

  val vertex_property = Map[String, Int]()

  def zero(initialValue: Map[String,Int]): Map[String,Int] = {
    Map.empty[String,Int]
  }

  def addInPlace(t1: Map[String, Int], t2: Map[String, Int]): Map[String, Int] = {
    for ((k, v) <- t2) {
      t1.get(k) match {
        case None => t1(k) = v
        case Some(e) => t1(k) = e + v
      }
    }

    t1
  }
}



object partB_App2_q4 {

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
    var verticesContentAcc = sc.accumulator(ListAccumulator4.zero(Nil))(ListAccumulator4)

    var i = 0L //Vertex Indicies
    while (directory_iterator.hasNext()) {
      //Get the path of the file
      var path = directory_iterator.next().getPath().toString

      //Create an accumulator which stores words of the file in a Map[key:word, value:occurance count default 1]
      val AccumulatorFileContentsToMap = sc.accumulator(MapAccumulator.zero(Map.empty[String,Int]))(MapAccumulator)

      //Open the textFile in SparkContext
      var TimeLineFile = sc.textFile(path)

      //Fetch each word and insert into the Map Accumulator
      TimeLineFile.foreach( x => AccumulatorFileContentsToMap += Map(x.toString -> 1))

      //Insert the Map into the Vertices content Accumulator
      val elem  = List[(Long, Map[String,Int])]((i, AccumulatorFileContentsToMap.value))
      verticesContentAcc.add(elem)

      //Increment the Vertex index
      i += 1L
    }

    //Create a Vertices RDD from the Accumulator of List[Long,HashSet[String])]
    val verticesRDD = sc.parallelize(verticesContentAcc.value)

    //Do a cartesian to check if a vertex has common words with any other vertex
    val verticesCartesianOutput = verticesRDD.cartesian(verticesRDD)

    //Do a HashSet of keys of Map stored in vertices to check if there are atleast one common words
    val vertexPairAtleastOneCommonWord = verticesCartesianOutput.map { case ((a: Long, b: Map[String,Int]), (c: Long, d: Map[String,Int]))
    => (a, c, Math.min(b.keySet.intersect(d.keySet).size,1))
    }

    //Filter entries which doesn't have atleast one common word
    val filteredEdgeRDD = vertexPairAtleastOneCommonWord.filter{ case (a: Long, b: Long, c: Int) => c != 0 && a!=b}

    //Change the filteredEdgeRDD to triplet of Edge type
    val EdgeRDD = filteredEdgeRDD.map{case (a: Long,b:Long, c: Int)=> Edge(a,b, c)}

    //--------------------------------Graph Generation Ends---------------------------------------------
    //Create the graph from verticesRDD and EdgeRDD
    val graph: Graph[Map[String,Int], Int] = Graph(verticesRDD, EdgeRDD)

    //Reduce function for Vertices which joins all the map across vertices
    //such that for word already present in Map then increment the count
    //else insert into the map
    def WordsCountMap(t1: (VertexId, Map[String, Int]), t2: (VertexId, Map[String, Int])): (VertexId,Map[String, Int]) = {
      for ((k, v) <- t2._2) {
        t1._2.get(k) match {
          case None => t1._2(k) = v
          case Some(e) => t1._2(k) = e + v
        }
      }
      t1
    }

    val FinalWordsCount:(VertexId,Map[String, Int]) = graph.vertices.reduce(WordsCountMap)

    //The key value pair with highest count
    println("==============================================")
    println("Application2 Question 4: Most popular word" + FinalWordsCount._2.max)
    println("==============================================")


    sc.stop()

  }
}
