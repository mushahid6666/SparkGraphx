/**
 * Created by mushahidalam on 11/27/16.
 */

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
import scala.collection.mutable.Map


object ListAccumulator5 extends AccumulatorParam[List[(Long, Map[String,Int])]] {

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

object MapAccumulator2 extends AccumulatorParam[Map[String,Int]] {

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

object Extra_credit_q2 {

  def main(args : Array[String]): Unit = {


    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("App")
    //      .setMaster("spark://10.254.0.53:7077")
    //      .setAppName("App")
    //      .set("spark.driver.memory", "8g")
    //      .set("spark.driver.cores", "2")
    //      .set("spark.eventLog.enabled", "true")
    //      .set("spark.eventLog.dir", "hdfs:/tmp/spark-events")
    //      .set("spark.executor.memory", "4g")
    //      .set("spark.executor.cores", "1")
    //      .set("spark.executor.instances","20")
    //      .set("spark.task.cpus", "1")

    var i = 0L

    val sc = new SparkContext(conf)

    //Set the configuration of the file system
    val fs_conf = new Configuration()
    //    fs_conf.set("fs.defaultFS", "hdfs://10.254.0.53:8020")
    val fs = FileSystem.get(fs_conf)


    //Open the directory of timeline files
    //    val path = new Path("storm_output_words")
    val path = new Path("/Users/mushahidalam/workspace/GraphX/data/tmp")
    val file1_iterator = fs.listFiles(path, false)

    //    var vertexArray: Array[Edge[Int]] = new Array[Edge[Int]](0)

    var vertexArray = sc.accumulator(ListAccumulator5.zero(Nil))(ListAccumulator5)

    while (file1_iterator.hasNext()) {
      var path = file1_iterator.next().getPath().toString


      val AccumulatorFileContentsToMap = sc.accumulator(MapAccumulator2.zero(Map.empty[String,Int]))(MapAccumulator2)

      var TimeLineFile = sc.textFile(path)

      TimeLineFile.foreach( x => AccumulatorFileContentsToMap += Map(x.toString -> 1))

      val elem  = List[(Long, Map[String,Int])]((i, AccumulatorFileContentsToMap.value))
      vertexArray.add(elem)

      i += 1L
    }

    val vertexRDD = sc.parallelize(vertexArray.value)


    val verticescartesianoutput = vertexRDD.cartesian(vertexRDD)

    val vertexPairAtleastOneCommonWord = verticescartesianoutput.map { case ((a: Long, b: Map[String,Int]), (c: Long, d: Map[String,Int]))
    => (a, c, Math.min(b.keySet.intersect(d.keySet).size,1))
    }

    val filteredEdgeRDD = vertexPairAtleastOneCommonWord.filter{ case (a: Long, b: Long, c: Int) => c != 0 && a!=b}
    //
    val EdgeRDD = vertexPairAtleastOneCommonWord.map{case (a: Long,b:Long, c: Int)=> Edge(a,b, c)}

    val graph: Graph[Map[String,Int], Int] = Graph(vertexRDD, EdgeRDD)

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

    val PopularWord = FinalWordsCount._2.max

    println(PopularWord)
    val ResultVertices = graph.vertices.filter{case (x:(VertexId,Map[String,Int])) => if(x._2.get(PopularWord._1) != None ) true else false }

    ResultVertices.foreach(println)
    //    val output_path = new Path("/home/ubuntu/output/final_output.txt")
    //    val output_path = new Path("/Users/mushahidalam/workspace/GraphX/data/output/q3_output.txt")
    //
    //    val output_file = fs.create(output_path)
    //    output_file.writeLong(Edgecount)
    //    output_file.close()
    //
    //    println(Edgecount)

    sc.stop()

  }
}
