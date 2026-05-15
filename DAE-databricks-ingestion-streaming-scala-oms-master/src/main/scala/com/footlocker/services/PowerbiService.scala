//package com.footlocker.services
//
//import com.microsoft.aad.adal4j.{AuthenticationContext, ClientCredential}
//import org.apache.http.client.methods.HttpPost
//import org.apache.http.entity.StringEntity
//import org.apache.http.impl.client.HttpClientBuilder
//import org.apache.spark.sql.DataFrame
//
//import scala.util.{Failure, Success, Try}
//
//trait PowerbiService {
//
////  // Returning T, throwing the exception on failure
////  @annotation.tailrec
////  private def retry[T](n: Int, delay_secs: Int)(fn: => T): T = {
////    Try { fn } match {
////      case Success(x) => x
////      case _ if n > 1 => {
////        println(s"Process failed. Retrying.")
////        Thread.sleep(delay_secs*1000)
////        retry(n - 1, delay_secs)(fn)
////      }
////      case Failure(e) => throw e
////    }
////  }
////
////  private def split_arr(xs: Array[String], limit: Int): List[Array[String]] = {
////    // Split string array into n number of subsets where n = xs.length / limit
////    if (xs.length <= limit){
////      return List(xs)
////    }
////    else{
////      return (xs take limit) +: split_arr(xs drop limit, limit)
////    }
////  }
////
////  def powerbipush(pbi_post_rows: String, df: DataFrame,client_id:String,client_secret:String, limit:Int=5000, delay_secs:Int=1): Unit ={
////
////    val delay_ms = delay_secs * 1000
////    val all_records = df.toJSON.collect()
////
////    // Split json array into multiple payloads based on limit
////    val json_splits = split_arr(all_records, limit)
////
////    // Setup service to acquire access token
////    val service: java.util.concurrent.ExecutorService = java.util.concurrent.Executors.newFixedThreadPool(1)
////    val httpClient =  HttpClientBuilder.create().build()
////    val authority_url = "https://login.windows.net/footlocker.com/"
////    var authToken:String = null
////    var attempts = 0
////
////    // Try retrieving access token
////    retry(5, delay_secs=10){
////      attempts += 1
////      println(s"Attempting to acquire Power BI access token. Attempt $attempts")
////      val context: AuthenticationContext = new AuthenticationContext(authority_url, true, service)
////      val tokenContext = context.acquireToken("https://analysis.windows.net/powerbi/api", new ClientCredential(client_id, client_secret), null)
////      authToken = tokenContext.get().getAccessToken
////      println("Access token acquired.")
////    }
////    var count = 0
////    // create POST request for each payload
////    for (payload <- json_splits) {
////      if (payload.length > 0) {
////        count += 1
////        println(s"Payload $count")
////        val jsonString = "{ \"rows\" : " + payload.mkString("[",",","]") +" }"
////
////        // Retry any failed posts.
////        retry(5, delay_secs=10) {
////          val httpPost = new HttpPost(pbi_post_rows)
////          httpPost.setEntity(new StringEntity(jsonString))
////          httpPost.setHeader("Accept", "application/json")
////          httpPost.setHeader("Content-Type", "application/json")
////          httpPost.setHeader("Authorization", "Bearer " + authToken);
////          val response = httpClient.execute(httpPost)
////          println(response.toString())
////          if(response.getStatusLine.getStatusCode != 200){
////            println("Failed reponse.")
////            throw new Exception(response.toString())
////          }
////
////          // Print Response to console for helpful message
////
////        }
////        // Delay used for timing requests
////        if (delay_secs != 0){
////          Thread.sleep(delay_ms)
////        }
////      }
////
////
////    }
////
////    httpClient.close()
////    service.shutdown()
////
////  }
//
//}