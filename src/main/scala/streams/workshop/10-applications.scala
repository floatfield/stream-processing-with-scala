package streams.workshop

import zio._
import zio.stream._

object Applications {
  // 1. Write a layer that represents a database client that supports
  // streaming results out of the database. Be sure to create an accessor
  // for streaming SQL results out using ZStream.accessStream.
  // Pattern: using a stream produced by a module
  class DatabaseClient(data: Ref[Map[String, String]]) {

    def readData: ZStream[Any, Nothing, (String, String)] = ZStream.fromIterableZIO(data.get)
    def close: UIO[Unit]                                  = ZIO.unit
  }
  object DatabaseClient {
    def make = Ref.make(Map[String, String]()).map(new DatabaseClient(_))
    def live = ZLayer.fromZIO(make)
  }

  /*  class DatabaseClient(data: Ref[Map[String, String]]) {

    def readData: ZStream[Any, Nothing, (String, String)] = ZStream.fromIterableM(data.get)
    def close: UIO[Unit]                                  = UIO.unit
  }
  object DatabaseClient {
    def make = Ref.make(Map[String, String]()).map(new DatabaseClient(_)).toManaged_
  } */

  // 2. Run these 3 streams in the main entrypoint of an application such that they
  // will run in the background, and any error that occurs in them will cause the
  // application to shut down.
  // Pattern: direct composition of streams into the application
  val kafkaConsumerStream =
    ZStream.acquireReleaseWith(Console.printLine("Opening Kafka consumer"))(_ =>
      Console.printLine("Closing Kafka consumer").orDie
    ) *>
      ZStream.repeatZIOWithSchedule(
        Random.nextIntBetween(0, 11).flatMap { i =>
          if (i < 7) Console.printLine("Processing record ...")
          else ZIO.fail(new RuntimeException("Kafka failure!"))
        },
        Schedule.fixed(2.seconds).jittered
      )
  val incomingTcpDataStream =
    ZStream.acquireReleaseWith(Console.printLine("Opening TCP data stream"))(_ =>
      Console.printLine("Closing TCP data stream").orDie
    ) *>
      ZStream.repeatZIOWithSchedule(
        Random.nextIntBetween(0, 11).flatMap { i =>
          if (i < 7) Console.printLine("Connection received, streaming to S3")
          else ZIO.fail(new RuntimeException("TCP failure!"))
        },
        Schedule.fixed(10.seconds).jittered
      )
  val httpServer = ZStream
    .acquireReleaseWith(Console.printLine("Listening on port 8080"))(_ =>
      Console.printLine("Unbinding HTTP server").orDie
    )
    .mapZIO(_ => ZIO.never)

  val app: ZIO[Any, Nothing, ExitCode] =
    ZIO.raceAll(httpServer.runDrain, Seq(incomingTcpDataStream.runDrain, kafkaConsumerStream.runDrain)).exitCode
  //httpServer.drainFork(incomingTcpDataStream.drainFork(kafkaConsumerStream))

  // 3. Create a layer that sets up a background cache invalidation fiber.
  // Pattern: streams as background processes

  class Cache(ref: Ref[Map[String, String]], timeRef: Ref[Vector[String]]) {

    def get(key: String): UIO[Option[String]] =
      ref.get.map(_.get(key)) <* timeRef.update(_.filterNot(_ == key) :+ key)
    def set(key: String, value: String): UIO[Unit] =
      ref.update(_.updated(key, value)) <* timeRef.update(_.filterNot(_ == key) :+ key)
    def empty: UIO[Unit] = ref.set(Map.empty) *> timeRef.set(Vector.empty)
  }

  object Cache {
    def makeCache: UIO[Cache] =
      for {
        mapRef  <- Ref.make(Map.empty[String, String])
        timeRef <- Ref.make(Vector.empty[String])
        _ <- timeRef
              .updateAndGet(_.takeRight(1000))
              .flatMap(keys => mapRef.update(_.filter { case (key: String, _: String) => keys.contains(key) }))
              .schedule(Schedule.fixed(5000.millis))
              .fork
      } yield new Cache(mapRef, timeRef)
    def live = ZLayer.fromZIO(makeCache)
  }
}
