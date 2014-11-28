package smx.sql

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD

object TopHashtags {

  def main(args: Array[String]) {

    if (args.length != 1) {
      val clazz = this.getClass.getSimpleName.replace("$", "")

      println(s"""
          |FAIL: Missing file argument
          |Usage: ${clazz} <filename>
          |
	        |""".stripMargin)

      sys.exit(1)
    }

    val file = args(0)

    // Crea un SparkContext
    val sc = new SparkContext("local[*]", "JSON and SparkSQL")

    // Carga el contenido del Json como RDD de String, particiona en 8
    val jsonRDD: RDD[String] = sc.textFile(file, 8)

    // Inicia Spark SQL
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD

    // Convierte String RDD para JSON
    val json = sqlContext.jsonRDD(jsonRDD)

    // Ver el Schema Detectado
    println("Schema auto-detected from JSON structure")
    json.printSchema

    // Registra RDD como tabla para poder ser consumida por SQL
    json.registerAsTable("json_tweets")
    val tweetSQL = sqlContext.sql("SELECT  user.screen_name, timestamp_ms, text FROM json_tweets")

    // A partir del resultado de SQL, crea un nuevo RDD del tipo Tweet
    case class Tweet(screenName: String, date: Long, text: String)
    val tweets = tweetSQL.map(t => Tweet(t.getString(0), t.getString(1).toLong, t.getString(2)))

    // Extrae las hashtags del texto y las cuenta
    val hashtags = tweets.flatMap(tweet => extractHashtags(tweet.text)).map(hashtag => (hashtag.toLowerCase, 1))
    val frequence = hashtags.reduceByKey(_ + _)

    // Inverte la tupla para (frecuencia, palabra)
    val invFreq = frequence.map(_.swap)

    // Tomas las 20 mayores y las imprime
    invFreq.top(20).foreach(println)
  }

  // Función auxiliar para extraer Hashtags del texto del tweet
  def extractHashtags(text: String) = {
    val pattern = "(#[0-9a-zA-Z]+)".r
    val matches = pattern.findAllMatchIn(text)
    matches.map(_.toString)
  }

}