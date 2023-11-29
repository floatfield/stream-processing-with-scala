package streams.workshop

import zio.stream._
import java.nio.file.Path
import scala.io.Source
import java.io.FileReader
import zio._

object ControlFlow {
  // 1. Write a stream that reads bytes from one file,
  // then prints "Done reading 1", then reads another bytes
  //z/ from another file.
  def bytesFromHereAndThere(here: Path, bytesAmount: Int): ZStream[Any, Throwable, Char] =
    ZStream
      .acquireReleaseWith(ZIO.succeed(Source.fromFile(here.toAbsolutePath.toFile)))(fr => ZIO.succeed(fr.close()))
      .flatMap(fr =>
        ZStream.fromZIOOption(
          for {
            arr       <- ZIO.succeed(Array.ofDim[Char](bytesAmount))
            charsRead <- ZIO.attempt(fr.reader().read(arr)).mapError(Option(_))
            res       <- if (charsRead == -1) ZIO.fail(None) else ZIO.succeed(Chunk.fromArray(arr))
          } yield res
        )
      )
      .flattenChunks
      .tap(c => Console.print(c.toString))

  val realBytesFromHereAndThere: ZStream[Any, Throwable, Char] =
    bytesFromHereAndThere(Path.of("some.txt"), 1024) ++
      ZStream.fromZIO(Console.printLine("Done reading 1")).drain ++
      bytesFromHereAndThere(Path.of("there.txt"), 1024)

  // 2. What would be the difference in output between these two streams?
  val output1 =
    ZStream("Hello", "World").tap(Console.printLine(_)).drain ++
      ZStream.fromZIO(Console.printLine("Done printing!")).drain

  val output2 =
    ZStream("Hello", "World").tap(Console.printLine(_)).drain *>
      ZStream.fromZIO(Console.printLine("Done printing!")).drain

  // 3. Read 4 paths from the user. For every path input, read the file in its entirety
  // and print it out. Once done printing it out, log the file name in a `Ref` along with
  // its character count. Once the stream is completed, print out the all the files and
  // counts.
  case class FileSummary(name: String, count: Long)
  val read4Paths: ZStream[Any, Throwable, FileSummary] = for {
    path             <- ZStream.fromZIO(Console.readLine).take(4)
    fileStreamReader = new FileReader(path)
    summary <- ZStream.fromZIO(
                ZStream.fromReader(fileStreamReader).tap(Console.print(_)).runCount.map(FileSummary(path, _))
              )

  } yield summary

  val eff: Task[Unit] = for {
    summaries <- read4Paths.runCollect
    _         <- ZIO.foreach(summaries)(Console.printLine(_))
  } yield ()
}

object StreamErrorHandling {
  // 1. Modify this stream, which is under our control, to survive transient exceptions.
  trait TransientException { self: Exception => }
  def query: RIO[Random, Int] = Random.nextIntBetween(0, 11).flatMap { n =>
    if (n < 2) ZIO.fail(new RuntimeException("Unrecoverable"))
    else if (n < 6) ZIO.fail(new RuntimeException("recoverable") with TransientException)
    else ZIO.succeed(n)
  }

  val queryResults: ZStream[Random, Throwable, Int] = ZStream.repeatZIO(query) ?

  // 2. Apply retries to the transformation applied during this stream.
  type Lookup = Lookup.Service
  object Lookup {
    trait Service {
      def lookup(i: Int): Task[String]
    }

    def lookup(i: Int): RIO[Lookup, String] = ZIO.environmentWithZIO[Lookup](_.get.lookup(i))

    def live: ZLayer[Any, Nothing, Lookup] =
      ZLayer.succeed { i =>
        if (i < 5) ZIO.fail(new RuntimeException("Lookup failure"))
        else ZIO.succeed("Lookup result")
      }
  }

  val queryResultsTransformed: ZStream[Random with Lookup, Throwable, Int] =
    ZStream.repeatZIO(query).mapZIO(i => Lookup.lookup(i).map((i, _))) ?

  // 3. Switch to another stream once the source fails in this stream.
  val failover: ZStream[Any, ???, ???] =
    ZStream(ZIO.succeed(1), ZIO.fail("Boom")).mapZIO(identity) ?

  // 4. Do the same, but when the source fails, print out the failure and switch
  // to the stream specified in the failure.
  case class UpstreamFailure[R, E, A](reason: String, backup: ZStream[R, E, A])
  val failover2: ZStream[Any, ???, ???] =
    ZStream(ZIO.succeed(1), ZIO.fail(UpstreamFailure("Malfunction", ZStream(2)))) ?

  // 5. Implement a simple retry combinator that waits for the specified duration
  // between attempts.
  def retryStream[R, E, A](stream: ZStream[R, E, A], interval: Duration): ZStream[???, ???, ???] = ???

  // 6. Measure the memory usage of this stream:
  val alwaysFailing = retryStream(ZStream.fail("Boom"), 1.millis)

  // 7. Surface typed errors as value-level errors in this stream using `either`:
  val eithers = ZStream(ZIO.succeed(1), ZIO.fail("Boom")) ?

  // 8. Use catchAll to restart this stream, without re-acquiring the resource.
  val subsection = ZStream
    .acquireReleaseWith(Console.printLine("Acquiring"))(_ => Console.printLine("Releasing").orDie)
    .flatMap(_ => ZStream(ZIO.succeed(1), ZIO.fail("Boom")))
}

object Concurrency {
  // 1. Create a stream that prints every element from a queue.
  val queuePrinter: ZStream[???, ???, ???] = ZStream.fromQueue(???) ?

  // 2. Run the queuePrinter stream in one fiber, and feed it elements
  // from another fiber. The latter fiber should run a stream that reads
  // lines from the user.
  val queuePipeline: ??? = ???

  // 3. Introduce the ability to signal end-of-stream on the queue pipeline.
  val queuePipelineWithEOS: ??? = ???

  // 4. Introduce the ability to signal errors on the queue pipeline.
  val queuePipelineWithEOSAndErrors: ??? = ???

  // 5. Combine the line reading stream with the line printing stream using
  // ZStream#drainFork.
  val backgroundDraining: ??? = ???

  // 6. Prove to yourself that drainFork terminates the background stream
  // when the foreground ends by:
  //   a. creating a stream that uses `ZStream.bracketExit` to print on interruption and
  //      delaying it with ZStream.never
  //   b. draining it in the background of another stream with drainFork
  //   c. making the foreground stream end after a 1 second delay
  val drainForkTerminates: ??? = ???

  // 7. Simplify our queue pipeline with `ZStream#buffer`.
  val buffered: ??? = ???

  // 8. Open a socket server, and wait and read from incoming connections for
  // 30 seconds.
  val timedSocketServer: ??? = ???

  // 9. Create a stream that emits once after 30 seconds. Apply interruptAfter(10.seconds)
  // and haltAfter(10.seconds) to it and note the difference in behavior.
  val interruptVsHalt: ??? = ???

  // 10. Use timeoutTo to avoid the delay embedded in this stream and replace the last
  // element with "<TIMEOUT>"
  val csvData = (ZStream("symbol,price", "AAPL,500") ++
    ZStream.fromZIO(Clock.sleep(30.seconds)).drain ++
    ZStream("AAPL,501")) ?

  // 11. Generate 3 random prices with a 5 second delay between each price
  // for every symbol in the following stream.
  val symbols = ZStream("AAPL", "DDOG", "NET") ?

  // 12. Regulate the output of this infinite stream of records to emit records
  // on exponentially rising intervals, from 50 millis up to a maximum of 5 seconds.
  def pollSymbolQuote(symbol: String): URIO[Random, (String, Double)] =
    Random.nextDoubleBetween(0.1, 0.5).map((symbol, _))
  val regulated = ZStream.repeatZIO(pollSymbolQuote("V")) ?

  // 13. Introduce a parallel db writing operator in this stream. Handle up to
  // 5 records in parallel.
  case class Record(value: String)
  def writeRecord(record: Record): RIO[Clock with Console, Unit] =
    Console.printLine(s"Writing ${record}") *> Clock.sleep(1.second)

  val dbWriter = ZStream.repeatZIO(Random.nextString(5).map(Record(_))) ?

  // 14. Test what happens when one of the parallel operations encounters an error.
  val whatHappens =
    ZStream(
      Clock.sleep(5.seconds).onExit(ex => Console.printLine(s"First element exit: ${ex.toString}").orDie),
      Clock.sleep(5.seconds).onExit(ex => Console.printLine(s"Second element exit: ${ex.toString}").orDie),
      Clock.sleep(1.second) *> ZIO.fail("Boom")
    ).mapZIOPar(3)(identity(_))
}

object StreamComposition {
  // 1. Merge these two streams.
  val one    = ZStream.repeatWithSchedule("left", Schedule.fixed(1.second).jittered)
  val two    = ZStream.repeatWithSchedule("right", Schedule.fixed(500.millis).jittered)
  val merged = ???

  // 2. Merge `one` and `two` above with the third stream here:
  val three       = ZStream.repeatWithSchedule("middle", Schedule.fixed(750.millis).jittered)
  val threeMerges = ???

  // 3. Emulate drainFork with mergeTerminateLeft.
  val emulation: ??? =
    ZStream
      .acquireReleaseWith(Queue.bounded[Long](16))(_.shutdown)
      .flatMap { queue =>
        val writer = ZStream.repeatZIOWithSchedule(Clock.nanoTime flatMap queue.offer, Schedule.fixed(1.second))
        val reader = ZStream.fromQueue(queue).tap(l => Console.printLine(l.toString))

        val (_, _) = (writer, reader)

        ZStream.unit
    } ?

  // 4. Sum the numbers between the two streams, padding the right one with zeros.
  val left  = ZStream.range(1, 8)
  val right = ZStream.range(1, 5)

  val zipped = ???

  // 5. Regulate the output of this stream by zipping it with another stream that ticks on a fixed schedule.
  val highVolume = ZStream.range(1, 100).forever ?

  // 6. Truncate this stream by zipping it with a `Take` stream that is fed from a queue.
  val toTruncate = ZStream.range(1, 100).forever ?

  // 7. Perform a deterministic merge of these two streams in a 1-2-2-1 pattern.
  val cats = ZStream("bengal", "shorthair", "chartreux").forever
  val dogs = ZStream("labrador", "poodle", "boxer").forever

  // 8. Write a stream that starts reading and emitting DDOG.csv every time the user
  // prints enter.
  val echoingFiles = ???

  // 9. Run a variable number of background streams (according to the parameter) that
  // perform monitoring. They should interrupt the foreground fiber doing the processing.
  def doMonitor(id: Int) =
    ZStream.repeatZIOWithSchedule(
      Random.nextIntBetween(1, 11).flatMap { i =>
        if (i < 8) Console.printLine(s"Monitor OK from ${id}")
        else ZIO.fail(new RuntimeException(s"Monitor ${id} failed!"))
      },
      Schedule.fixed(2.seconds).jittered
    )

  val doProcessing =
    ZStream.repeatZIOWithSchedule(
      Random.nextDoubleBetween(0.1, 0.5).map(f => s"Sample: ${f}"),
      Schedule.fixed(1.second).jittered
    )

  def runProcessing(nMonitors: Int) = {
    val _ = nMonitors
    doProcessing ?
  }

  // 10. Write a stream that starts reading and emitting DDOG.csv every time the user
  // prints enter, but only keeps the latest 5 files open.
  val echoingFilesLatest = ???
}
