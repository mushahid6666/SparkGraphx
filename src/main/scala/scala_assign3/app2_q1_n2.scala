package scala_assign3


/**
 * Created by mushahidalam on 11/20/16.
 */

import java.io.{PrintWriter, FileWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.HashSet
import scala.io.Source
import java.io._

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD;

object app2_q1_n2 {

  def main(args : Array[String]): Unit = {

///*    val map1 = scala.collection.mutable.HashMap.empty[Int, String]
//    map1 += (1 -> "make a web site")
//    map1 += (3 -> "profit!")
//    val map2 = scala.collection.mutable.HashMap.empty[Int, String]
//    map2 += (1 -> "make a web site")
//    map2 += (3 -> "Gandu")
//    val map4 = (map1.keySet -- map2.keySet) ++ (map2.keySet -- map1.keySet)
//    println(map4)*/
//
    val conf = new SparkConf()
      .setMaster("spark://10.254.0.53:7077")
//      .setMaster("local[1]")
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

    val fs = FileSystem.get(new Configuration())
    val path = new Path("hdfs:///user/ubuntu/storm_output_words/")
//    val path = new Path("/Users/mushahidalam/workspace/GraphX/data/tmp/")

    val directory_iterator = fs.listFiles(path, false)
    val file2_iterator = fs.listFiles(path, false)

//    val fw = new PrintWriter(new File("edge_file_question1.txt"))

    //vertex_ids
    var i:Long = 0L
    var j:Long = 0L

    var edgeArray: Array[Edge[Int]] = new Array[Edge[Int]](0)

    //Generate vertex array
    var vertexArray: Array[(Long,Int)] = new Array[(Long, Int)](0)

    while (directory_iterator.hasNext()) {
      var path = directory_iterator.next().getPath().toString
      val file1:RDD[(Char)] = sc.parallelize(path).toJavaRDD()
//        .toJavaRDD().persist(StorageLevel.MEMORY_ONLY)

      vertexArray +:= (i, Source.fromURL(path).getLines.size)

      while (file2_iterator.hasNext()) {
        val file2:RDD[(Char)] = sc.parallelize(file2_iterator.next().getPath().toString)

        if (i != j) {

          val file3 = file1.map(s => (s, 1))
          val file4 = file2.map(s => (s, 1))

          var count = file3.join(file4).count()


          if (count > 0){
//              var output =i+" "+j+"\n"
//              fw.write(output)
            edgeArray +:= Edge(i,j,1)
            edgeArray +:= Edge(j,i,1)
          }

          print(directory_iterator.toString, file2_iterator.toString,count)
        }
        j+=1
      }
      i+=1
    }
//    fw.close()
//
// Uncomment below code to debug generated edge and vertex array
    for ( x <- edgeArray ) {
      print( " " + x )
    }


    val vertexRDD: RDD[(Long, Int)] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    val graph: Graph[Int, Int] = Graph(vertexRDD, edgeRDD)

    var Edgecount = graph.triplets.filter(f => f.srcAttr > f.dstAttr).count()

    print(Edgecount)

    sc.stop()
    // (1,98) (0,70)
  }
  /*
  *
  *
  * */
}
