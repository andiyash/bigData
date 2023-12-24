package zoo

import scala.util.Random

object Main {
  val sleepTime = 5000

  def main(args: Array[String]): Unit = {
    println("Starting animal runner")

    val Seq(animalName, hostPort, partySize) = args.toSeq

    val animal = Animal(animalName, hostPort, "/zoo", partySize.toInt)

    try {
      // взаимодействие с ZooKeeper
      animal.enter()

      println(s"${animal.name} entered.")

      for (i <- 1 to Random.nextInt(100)) {
        Thread.sleep(sleepTime)

        println(s"${animal.name} is running...")
      }

      // выход из ZooKeeper
      animal.leave()

    } catch {
      case e: Exception => println("Animal was not permitted to the zoo. " + e)
    }
  }
}
