import twoPhaze.{Client, Coordinator}

import java.util.concurrent.Semaphore

object Main {
  def main(args: Array[String]): Unit = {
    com()
  }
  def com(): Unit = {
    val hostPort = "localhost:2181"
    val root = "/commit"
    val n_workers = 7
    val clients = new Array[Thread](n_workers)

    val coordinator = Coordinator(hostPort, root, n_workers)

    val coordinator_thread = new Thread(
      () => {
        coordinator.run()
      }
    )
    coordinator_thread.start()

    for (i <- 0 until n_workers) {
      clients(i) = new Thread(
        () => {
          val client = Client(i, hostPort, coordinator.coordinatorPath)
          client.run()
        }
      )
      clients(i).start()
    }
  }
  }