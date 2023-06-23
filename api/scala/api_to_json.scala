import scala.io.Source
import java.io.PrintWriter
import scala.util.{Try, Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._

import scala.language.postfixOps

import scala.concurrent.duration._

object Main extends App {
  val url = "https://randomuser.me/api/"
  val filePath = "data.json"

  val futureResponse: Future[String] = Future {
    Source.fromURL(url).mkString
  }

  val result = Try(Await.result(futureResponse, 5 seconds))

  result match {
    case Success(responseBody) =>
      val writer = new PrintWriter(filePath)
      writer.write(responseBody)
      writer.close()
      println("File saved successfully.")

    case Failure(exception) =>
      println(s"An error occurred: ${exception.getMessage}")
  }
}
