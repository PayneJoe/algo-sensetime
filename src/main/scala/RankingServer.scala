package com.sensetime.ad.algo.ranking

/**
  * Created by yuanpingzhou on 1/13/17.
  */

/*
object FeaturesIO{
  //
  import breeze.linalg.{DenseVector => BDV}
  //
  import com.sensetime.ad.algo.utils.Redis

  def writeRedisTask(nFeat: Int, ip: String, port: Int): Unit = {
    // connect
    val redisHandle = new Redis(ip, port)
    val rand = breeze.stats.distributions.Gaussian(0, 0.1)
    val values = BDV.rand(nFeat, rand)

    var data = Map[String, Double]()
    var i = 0
    while (i < nFeat) {
      data ++= Map(s"feat_${i}" -> values(i))
      i += 1
    }
    val startTime0 = System.currentTimeMillis()
    redisHandle.bunchWrite(data)
    redisHandle.disconnect()
    val endTime0 = System.currentTimeMillis()

    val timeVal0 = (endTime0 - startTime0) * 0.001
    println(f"write ${nFeat} records timeval ${timeVal0}%.2f(s)")
  }

  def readRedisTask(nFeat: Int, ip: String, port: Int) = {

    val redisHandle = new Redis(ip, port)
    val featKeys = (0 to (nFeat - 1)).map {
      v =>
        s"feat_${v}"
    }.toArray

    val startTime1 = System.currentTimeMillis()
    val result = redisHandle.bunchRead(featKeys)
    redisHandle.disconnect()
    val endTime1 = System.currentTimeMillis()

    val timeVal1 = (endTime1 - startTime1) * 0.001
    println(f"read ${nFeat}  records timeval ${timeVal1}%.2f(s)")

  }

  def main(args: Array[String]) = {

    // input
    val nFeat = 4000000
    val ip = "127.0.0.1"
    val port = 6379

    // write test
    writeRedisTask(nFeat,ip,port)
    // read test
    readRedisTask(nFeat,ip,port)
  }
}
*/

import org.http4s._
import org.http4s.dsl._

import scalaz.concurrent.Task
import org.http4s.server.{Server, ServerApp}
import org.http4s.server.blaze._
/*
import org.http4s.circe._
import io.circe.generic.auto._
import io.circe.syntax._
*/
import com.sensetime.ad.algo.ranking.RankingParameters._

object RankingServer extends ServerApp {

  // http server test with GET method
  val services = HttpService {
    case GET -> Root / "user=" / user_id => Ok(s"Hello , ${user_id}")
  }

  // http server test with POST method , whose body is formatted with json
  val jsonService = HttpService {
    /*
    // Handled with circe library , working not well with scala2.10.*
    case r @ POST -> Root / "list_request" =>
      r.as(jsonOf[ListRequest]).flatMap{
        listRequest =>
          val featList = listRequest.featList.split(",",-1)
          val ranked = featList.sortWith((s0: String,s1: String) => s0.compare(s1) > 0).mkString(",")
          Ok(ListResponse(listRequest.userId,listRequest.listSessionId,System.currentTimeMillis()/1000,ranked).asJson)
      }
    */

    // Handled with argonaut library , perfectly working both with scala 2.10.* and 2.11.*
    case req @ POST -> Root / "list_request" => {
      req.decode[ListRequest] {
        case data =>
          val featList = data.featList.split(",", -1)
          val ranked = featList.sortWith((s0: String, s1: String) => s0.compare(s1) > 0).mkString(",")
          Ok(ListResponse(data.userId, data.listSessionId, data.timestamp, ranked))
      }
    }
    // TODO : proper exception handling
    case _ =>
      Ok("ok")
  }

  override def server(args: List[String]): Task[Server] = {
    BlazeBuilder
      .bindHttp(1024, "localhost")
      .mountService(jsonService, "/")
      .start
  }
}
