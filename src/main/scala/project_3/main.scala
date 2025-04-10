package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main {
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def LubyMIS(graph: Graph[Int, Int]): Graph[Int, Int] = {
    var g = graph.mapVertices((id, _) => 0)
    var keepGoing = true
    var iter = 0
    val startTime = System.currentTimeMillis()

    while (keepGoing) {
      iter += 1
      val randGraph = g.mapVertices {
        case (id, attr) => if (attr == 0) scala.util.Random.nextDouble() else -1.0
      }

      val neighborMax = randGraph.aggregateMessages[Double](
        triplet => {
          if (triplet.srcAttr != -1.0 && triplet.dstAttr != -1.0) {
            triplet.sendToDst(triplet.srcAttr)
            triplet.sendToSrc(triplet.dstAttr)
          }
        },
        (a, b) => math.max(a, b)
      )

      val joined = randGraph.vertices.join(neighborMax)
      val candidateMIS = joined
        .filter { case (_, (myRand, nbrRand)) => myRand >= nbrRand && myRand != -1.0 }
        .mapValues(_ => 1)

      val allWithRand = randGraph.vertices
      val candidateWithoutNeighbors = allWithRand
        .subtractByKey(joined)
        .filter { case (_, rand) => rand != -1.0 }
        .mapValues(_ => 1)

      val newMIS = candidateMIS.union(candidateWithoutNeighbors)

      val withMIS = g.joinVertices(newMIS) {
        case (_, _, _) => 1
      }

      val neighborsToRemove = withMIS.aggregateMessages[Int](
        triplet => {
          if (triplet.srcAttr == 1 && triplet.dstAttr == 0) triplet.sendToDst(-1)
          if (triplet.dstAttr == 1 && triplet.srcAttr == 0) triplet.sendToSrc(-1)
        },
        (a, b) => -1
      )

      val updated = withMIS.joinVertices(neighborsToRemove) {
        case (_, oldAttr, msg) => if (oldAttr == 0) msg else oldAttr
      }

      g = updated

      val remaining = g.vertices.filter { case (_, attr) => attr == 0 }.count()
      keepGoing = remaining > 0
    }

    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    val misSize = g.vertices.filter { case (_, attr) => attr == 1 }.count()
//print statmentents added here and removed from main
    println("==================================")
    println(f"Luby's algorithm completed in $totalTime%.2f seconds.")
    println(s"Iterations: $iter")
    println(s"MIS size: $misSize")
    println("==================================")

    g
  }

  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    val invalidCount = g_in.triplets
      .filter(triplet => triplet.srcAttr == 1 && triplet.dstAttr == 1)
      .count()
    if (invalidCount > 0) return false

    val hasInMisNeighbor = g_in.aggregateMessages[Boolean](
      ctx => {
        if (ctx.srcAttr == 1) ctx.sendToDst(true)
        if (ctx.dstAttr == 1) ctx.sendToSrc(true)
      },
      (m1, m2) => m1 || m2,
      TripletFields.All
    )

    val nonMaximalCount = g_in.vertices
      .join(hasInMisNeighbor)
      .filter { case (_, (attr, hasNeighborInMIS)) => attr == 0 && !hasNeighborInMIS }
      .count()

    nonMaximalCount == 0
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    if (args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }

    if (args(0) == "compute") {
      if (args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val edges = sc.textFile(args(1)).map(line => {
        val x = line.split(","); Edge(x(0).toLong, x(1).toLong, 1)
      })
      val g = Graph.fromEdges[Int, Int](edges, 0, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)
      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    } else if (args(0) == "verify") {
      if (args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }
      val edges = sc.textFile(args(1)).map(line => {
        val x = line.split(","); Edge(x(0).toLong, x(1).toLong, 1)
      })
      val vertices = sc.textFile(args(2)).map(line => {
        val x = line.split(","); (x(0).toLong, x(1).toInt)
      })
      val g = Graph(vertices, edges, 0, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)
      val ans = verifyMIS(g)
      if (ans) println("****** YES IS A MIS *****") 
      else println("****** NOT A MIS ******")
    } else {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
  }
}
