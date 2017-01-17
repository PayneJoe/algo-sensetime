package com.sensetime.ad.algo.ranking

/**
  * Ranking client for Recommendation System , sending list request
  *
  * Created by yuanpingzhou on 1/15/17.
  */

import org.http4s._
import org.http4s.client.blaze._
import org.http4s.Uri
import org.http4s.argonaut._

//import org.http4s.circe._
//import io.circe.generic.auto._

import com.sensetime.ad.algo.ranking.RankingParameters._

object RankingClient {

  def main(args: Array[String]) {
    val httpClient = PooledHttp1Client()
    val feats = "feat1,feat0,feat2"
    // Handled with circe library , working not well with scala 2.10.*
    // val req = Request(uri = Uri.uri("http://localhost:1024/list_request"), method = Method.POST)
     // .withBody(ListRequest("joe","10101010",1484480518,feats))(jsonEncodeOf)

    // Handled with argonaut library , perfectly working both with scala 2.10.* and scala 2.11.*
    val req = Request(uri = Uri.uri("http://localhost:1024/list_request"), method = Method.POST)
                        .withBody(ListRequest("joe","10101010",1484480518,feats))
    try {
      val ret = httpClient.expect(req)(jsonOf[ListResponse]).run.copy()
      println(s"response time ${ret.timestamp} before ranked ${feats} after ranked ${ret.featList}")
    }
    catch {
      case e: Exception =>
        println(s"send message error : ${e.getMessage}")
    }
    finally {
      println("shutdown now ... ")
      httpClient.shutdownNow()
    }
  }
}
