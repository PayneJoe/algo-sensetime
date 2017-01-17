package com.sensetime.ad.algo.utils


/**
  * Created by yuanpingzhou on 1/13/17.
  */
import redis.clients.jedis.{Jedis, Response}
import scala.collection.JavaConverters._

class Redis(private val host: String,private val port: Int) {

  private val handle = new Jedis(host,port)

  // get newest key from a sorted set
  def getKey(dbNum: Int): String = {
    val jd = this.handle
    var modelKey = ""
    try {
      jd.select(dbNum)
      modelKey = jd.zrange("model_index", -1, -1).iterator().next()
    }
    catch{
      case e: Exception =>
        println(s"Get key for the newest model , unfortunately error : ${e}")
        jd.disconnect()
        System.exit(1)
    }
    finally {
      jd.disconnect()
    }
    modelKey
  }

  // get one record with specific db and key
  def getRecord(dbNum: Int,keyStr: String): Map[String,String] = {
    val jd = this.handle
    var tryTimes = 3
    var flag = false
    var tmpResult: Response[java.util.Map[String,String]] = null
    while(tryTimes > 0 && !flag){
      try{
        val pp = jd.pipelined()
        pp.select(dbNum)
        tmpResult = pp.hgetAll(keyStr)
        pp.sync()
        flag = true
      }
      catch{
        case e: Exception =>
          flag = false
          println(s"Get the newest model , unfortunately error : ${e}")
          tryTimes -= 1
      }
      finally {
        jd.disconnect()
      }
    }
    tmpResult.get().asScala.toMap
  }

  def disconnect() ={
    this.handle.disconnect()
  }
}
