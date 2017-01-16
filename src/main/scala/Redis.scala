package com.sensetime.ad.algo.utils


/**
  * Created by yuanpingzhou on 1/13/17.
  */
import redis.clients.jedis.{Jedis, Response}

class Redis(private val ip: String,private val port: Int) {

  val handle = new Jedis(ip,port)

  def bunchWrite(data: Map[String,Double]) = {
    val jd = handle
    var tryTimes = 3
    var flag = false
    while(tryTimes > 0 && !flag){
      try{
        val pp = jd.pipelined()
        data.foreach{
          case (k,v) =>
            pp.set(k,v.toString)
        }
        pp.sync()
        flag = true
      }
      catch{
        case e: Exception =>
          flag = false
          println(s"redis timeout ${e}")
          tryTimes -= 1
      }
      finally {
        jd.disconnect()
      }
    }
  }

  def bunchRead(keys: Array[String]): Map[String,Response[String]] = {

    val jd = handle
    var tmpResult = Map[String, Response[String]]()
    var tryTimes = 3
    var flag = false
    while(tryTimes > 0 && !flag) {
      try{
        val pp = jd.pipelined()
        var i = 0
        while(i < keys.length){
          tmpResult ++= Map(keys(i) -> pp.get(keys(i)))
          i += 1
        }
        pp.sync()
        flag = true
      }catch {
        case e: Exception => {
          flag = false
          println("Redis-Timeout" + e)
          tryTimes = tryTimes - 1
        }
      }finally{
        jd.disconnect()
      }
    }

    tmpResult

  }

  def disconnect() ={
    this.handle.disconnect()
  }
}
