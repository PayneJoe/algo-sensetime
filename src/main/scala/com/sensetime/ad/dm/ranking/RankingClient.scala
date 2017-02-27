package com.sensetime.ad.dm.ranking

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

import com.sensetime.ad.dm.ranking.RankingParameters._

object RankingClient {

  def main(args: Array[String]) {
    val httpClient = PooledHttp1Client()
    val feats = "20161202104440887653961,20161202104553729282031,20161201192156529686624,20161201115822064345323,20161201192132784915483"
    // Handled with circe library , working not well with scala 2.10.*
    // val req = Request(uri = Uri.uri("http://localhost:1024/list_request"), method = Method.POST)
     // .withBody(ListRequest("joe","10101010",1484480518,feats))(jsonEncodeOf)

    // Handled with argonaut library , perfectly working both with scala 2.10.* and scala 2.11.*
    val req = Request(uri = Uri.uri("http://localhost:1024/list_request"), method = Method.POST)
                        .withBody(ListRequest("0e25d1dff0294c2f9bc71da9ca7060fd_105110453","session_id_10101010",1484480518,feats))
    try {
      val ret = httpClient.expect(req)(jsonOf[ListResponse]).run.copy()
      println(s"response time ${ret.timestamp} before ranked ${feats} after ranked ${ret.featList}")
    }
    catch {
      case e: Exception =>
        httpClient.shutdownNow()
        println(s"send message error : ${e.getMessage}")
    }
    finally {
      println("shutdown now ... ")
      httpClient.shutdownNow()
    }
  }
}
