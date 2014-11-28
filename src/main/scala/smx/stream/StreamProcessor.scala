package smx.stream

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import java.util.Date
import smx.utils.Log

object StreamProcessor {

  def main(args: Array[String]) {

    println(s"""
        |Streaming Processor from Socket
	|WISIT2014
        |""".stripMargin)

    Log.setStreamingLogLevels()

    val sc = new SparkContext("local[*]", "StreamProcessor")

    // Define una ventana de 5 segundos
    val ssc = new StreamingContext(sc, Seconds(5))

    // Create a DStream que se conecta al puerto 7777 del server
    val lines = ssc.socketTextStream("localhost", 7777)

    // procesa el contenido
    processDStream2(lines)

    // start our streaming context and wait for it to "finish"
    ssc.start()

    // Wait for 10 seconds then exit.
    // ssc.awaitTermination(10000)

    ssc.awaitTermination
    ssc.stop()

  }

  def processDStream(lines: DStream[String]) {
    lines.foreach { rdd =>
      val count = rdd.count()
      println(s"${new Date().toLocaleString()} Total lines on this batch: ${count}")
    }
    println("=" * 30)
  }

  def processDStream2(lines: DStream[String]) {
    lines.foreach { rdd =>

    // Using Spark SQL
    val sqlContext = new org.apache.spark.sql.SQLContext(rdd.context)
    import sqlContext.createSchemaRDD

    // convert String into JSON
    val json = sqlContext.jsonRDD(rdd)

    // Register Table and Select it
    json.registerAsTable("json_tweets")
    val tweetSQL = sqlContext.sql("SELECT  user.screen_name, timestamp_ms, text FROM json_tweets")

    // Create a object Tweet and a new RDD
    case class Tweet(screenName: String, date: Long, text: String)
    val tweets = tweetSQL.map(t => Tweet(t.getString(0), t.getString(1).toLong, t.getString(2)))

    val hashtags = tweets.flatMap(tweet => extractHashtags(tweet.text)).map(hashtag => (hashtag.toLowerCase, 1))
    val frequence = hashtags.reduceByKey(_ + _)

    // Inverte la tupla para (frecuencia, palabra)
    val invFreq = frequence.map(_.swap)

    // Tomas las 20 mayores y las imprime
    invFreq.top(20).foreach(println)

    }
    
    def extractHashtags(text: String) = {
      val pattern = "(#[0-9a-zA-Z]+)".r
      val matches = pattern.findAllMatchIn(text)
      matches.map(_.toString)
    }
  }

}