/**
  * Created by yuanpingzhou on 10/15/16.
  */
import org.apache.spark.{SparkContext, SparkConf}

object wordcount {
    def main(args:Array[String]) : Unit = {
      //System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
      val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
      val sc = new SparkContext(conf)
      val tf = sc.textFile(args(0))
      val splits = tf.flatMap(line => line.split(" ")).map(word =>(word,1))
      val counts = splits.reduceByKey((x,y)=>x+y)
      splits.saveAsTextFile(args(1))
      counts.saveAsTextFile(args(2))
    }
}
