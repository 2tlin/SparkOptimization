package homeworks

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object Recap extends App {
  println("Hello Spark Optimization")

  // Future
  val aFuture = Future {
    42
  }

  aFuture.onComplete {
    case Success(rightAnswer) => println(s"$rightAnswer is the meaning of everything")
    case Failure(ex) => println(ex)
  }

  // Partial Functions
  val aPrtialFunction: PartialFunction[Int, Int]  = {
    case 1  => 1
    case 2  => 2
    case _ => -1000000
  }

  println(aPrtialFunction(3))


}
