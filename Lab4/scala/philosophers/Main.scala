package philosophers

import java.util.concurrent.{Executors, Semaphore}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object Main {
  def main(args: Array[String]): Unit = {
    philosophers()
  }

  def philosophers(): Unit = {
    val hostPort = "localhost:2181"
    val root = "/philosophers"
    val places = 9
    val iterations = 5

    val forks = new Array[Semaphore](places)
    for (i <- 0 until places) {
      forks(i) = new Semaphore(1)
    }

    val executorService = Executors.newFixedThreadPool(places)
    val executionContext = ExecutionContext.fromExecutorService(executorService)

    try {
      val philosopherTasks = for (i <- 0 until places) yield {
        Future {
          val philosopher = Philosopher(i, hostPort, root, forks(i), forks((i + 1) % places), iterations)
          philosopher.run() // Запускаем выполнение поедания и размышления
        }(executionContext)
      }

      // Ждем, пока все задачи завершатся
      Await.result(Future.sequence(philosopherTasks), Duration.Inf)
    } finally {
      executorService.shutdown()
    }
  }
}
