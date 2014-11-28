package smx.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCount {

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

    // create a SparkContext
    val sc = new SparkContext("local[*]", "WordCount")

    // Crear un RDD con el contenido del archivo don-quijote.txt.gz
    val docs = sc.textFile(file)

    // Convertir el texto de cada linea en minusculas
    val lower = docs.map(line => line.toLowerCase)

    // Separa cada linea del texto por palabras (strings separadas por espacio)
    // aplana/achata los arrays del comando split
    val words = lower.flatMap(line => line.split("\\s+"))

    // Crea un tuple (palabra, frecuencia)
    // la frecuencia inicial de cada palabra es 1
    val counts = words.map(word => (word, 1))

    // Agrupa por palabra y suma las frecuencias
    val freq = counts.reduceByKey(_ + _)

    // Inverte la tupla para (frecuencia, palabra)
    val invFreq = freq.map(_.swap)

    // Tomas las 20 mayores y las imprime
    invFreq.top(20).foreach(println)

  }

}