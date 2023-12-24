package philosophers

import java.util.concurrent.{Semaphore, Executors}
import scala.util.Random
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs, ZooKeeper}

case class Philosopher(id: Int, hostPort: String, root: String, leftFork: Semaphore, rightFork: Semaphore, iterations: Int) extends Watcher {
  val zooKeeper = new ZooKeeper(hostPort, 3000, this)
  val mutex = new Object()
  val philosopherPath = s"$root/philosopher_$id"

  if (zooKeeper == null) throw new Exception("zooKeeper is not initialized")

  override def process(event: WatchedEvent): Unit = {
    mutex.synchronized {
      mutex.notify()
    }
  }

  def run(): Unit = {
    for (_ <- 0 until iterations) {
      eating()
      thinking()
    }
  }

  def eating(): Unit = {
    zooKeeper.create(philosopherPath, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)

    mutex.synchronized {
      rightFork.acquire()
      println(s"Philosopher $id takes right fork")
      leftFork.acquire()
      println(s"Philosopher $id takes left fork")
      Thread.sleep((Random.nextInt(9) + 1) * 1000)
      rightFork.release()
      println(s"Philosopher $id puts right fork")
      leftFork.release()
      println(s"Philosopher $id puts left fork")
    }
  }

  def thinking(): Unit = {
    println(s"Philosopher $id is thinking")
    zooKeeper.delete(philosopherPath, -1)
    Thread.sleep((Random.nextInt(9) + 1) * 1000)
  }
}
