package smx.utils

object SocketServer {
  import java.net._
  import java.io._
  import scala.io.Source

  // A dummy socket server
  // to simulate a file stream
  //
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
    val PORT = 7777

    println(s"""
        |SocketServer localhost:$PORT
        |Source file: $FILE
        |
        |""".stripMargin)

    val server = new ServerSocket(7777)
    val s = server.accept()
    val out = new PrintStream(s.getOutputStream())

    import scala.io.Source
    val lines = Source.fromFile(file).getLines()
    
    
    val rnd = new scala.util.Random

    println("Emitting lines: ")
    while (lines.hasNext) {
      val line = lines.next

      val info = if (line.length() > 30)
          line.substring(0, 30) + " ...")
        else
          line
      
      println(info)

      out.println(line)

      Thread.sleep(rnd.nextInt(750))
      out.flush()
    }

    s.close()
  }
}