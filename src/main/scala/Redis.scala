package com.sensetime.ad.algo.utils


/**
  * Created by yuanpingzhou on 1/13/17.
  */
import com.sensetime.ad.algo.utils.ExceptionPool.RankingException
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
        //  DEBUG
        // println(e.getMessage)
    }
    finally {
      // TODO
    }
    // assert result
    if((modelKey == null) || modelKey.isEmpty){
      throw new RankingException("get key failed")
    }
    else {
      modelKey
    }
  }

  // get one record with specific db and key
  def getRecord(dbNum: Int,keyStr: String): Map[String,String] = {
    val jd = this.handle
    var tryTimes = 3
    var flag = false
    var tmpResult: Response[java.util.Map[String,String]] = null
    // try more times
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
          tryTimes -= 1
      }
      finally {
        // TODO
      }
    }
    // assert result
    if((tmpResult == null) || (tmpResult.get().size() == 0)){
      throw new RankingException("get row failed")
    }
    else {
      tmpResult.get().asScala.toMap
    }
  }

  def disconnect() ={
    this.handle.disconnect()
  }
}
