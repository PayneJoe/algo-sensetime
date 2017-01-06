package com.sensetime.ad.algo.test


/**
  * Created by yuanpingzhou on 11/25/16.
  */
object PartitionTest {
  import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

  def main(args: Array[String]) ={

    if(args.length < 1) {
      println("param : mode ")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("PartitionTest").setMaster(args(0))
    val sc = new SparkContext(conf)

    val data = Array(("0","123"),("1","24"),("2","35"),("3","28"),("4","356"),("5","0"),("6","23"),("7","8"),("8","32"),("9","3467"))

    // Assume that our neighbor list was saved as a Spark objectFile
    val links = sc.parallelize(data)
      .partitionBy(new HashPartitioner(3))
      .persist()

    // Initialize each page's rank to 1.0; since we use mapValues, the resulting RDD
    // will have the same partitioner as links
    var ranks = links.mapValues(v => 1.0)

    //println(links.getNumPartitions)

    // Run 10 iterations of PageRank
    for (i <- 0 until 4) {
      val contributions = links.cogroup(ranks).flatMap {
        case (pageId, pair) =>
          val links = pair._1.head
          val rank = pair._2.head
          links.map(dest => (dest.toString, rank / links.size))
      }
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85*v)
      //println(ranks.collect())
    }

    // Write out the final ranks
    //ranks.saveAsTextFile("ranks")
  }
}
