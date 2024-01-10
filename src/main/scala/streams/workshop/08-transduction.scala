package streams.workshop

import zio._

import zio.stream._
import zio.Random

object AccumulatingMaps {
  // 1. Compute a running sum of this infinite stream using mapAccum.
  val numbers = ZStream.iterate(0)(_ + 1).mapAccum(0)((sum, next) => (sum + next, sum + next))

  // 2. Use mapAccum to pattern match on the stream and group consecutive
  // rising numbers.
  val risingNumbers = ZStream
    .repeatZIO(Random.nextIntBetween(0, 21))
    .mapAccum(Chunk[Int]()) { (chunk, next) =>
      if (chunk.isEmpty) (Chunk(next), None)
      else if (chunk.int(chunk.size - 1) <= next) (chunk.appended(next), None)
      else (Chunk(next), Some(chunk))
    }
    .collectSome
    .tap(chunk => Console.printLine(chunk.toString()))
    .flattenChunks

  // 3. Using mapAccumM, write a windowed aggregation function. Sum the
  // incoming elements into windows of N seconds.
  case class Record(value: Long)
  case class Windowed(windowStart: Long, windowEnd: Long, sum: Long)

  val records =
    ZStream.repeatZIOWithSchedule(
      Random.nextLongBetween(0, 100).map(Record(_)),
      Schedule.fixed(5.seconds).jittered
    )

  def windowed[R, E, A](interval: Duration, records: ZStream[R, E, A])(f: A => Long): ZStream[R, E, Windowed] = ???

  // 4. Implement state save/restore for your windowing stream.
  trait StateStore {
    def saveState(windowId: Long, l: Long): Task[Unit]
    def loadState: Task[Map[Long, Long]]
  }

  def windowedPersistent[R, E, A](interval: Duration, records: ZStream[R, E, A], state: StateStore)(
    f: A => Long
  ): ZStream[R, E, Windowed] = ???
}

object Transduction {
  case class Record(key: String, data: Long)
  def recordStream[R](schedule: Schedule[R, Any, Any]) =
    ZStream
      .repeatZIOWithSchedule(
        Random
          .shuffle(List("a", "b", "c", "d"))
          .map(_.head)
          .zipWith(Random.nextLongBetween(0, 15))(Record(_, _)),
        schedule
      )

  // 1. Batch this stream of records into maps of 2 records each, keyed by the
  // records' primary key. Keep the last record for every key. Use ZTransducer.collectAllToMapN.

  trait Database {
    def writeBatch(data: Map[String, Record]): RIO[Clock, Unit]
  }
  object Database {
    def make: Database = data => ZIO.attempt(println(s"Writing ${data}")).delay(1.second)
  }

  val batcher = ZSink.collectAllToMapN[Nothing, Record, String](n = 2)(_.key)((_, r2) => r2)

  val records: ZStream[Any, Nothing, Map[String, Record]] =
    recordStream(Schedule.forever)
      .transduce(batcher)

  // 2. Group the `records` stream according to their cost - the value of data - with
  // up to 32 units in total in each group. Use ZTransducer.foldWeighted.
  val recordsWeighted = recordStream(Schedule.forever).transduce {
    ZSink.foldWeighted(Chunk[Record]())((_: Chunk[Record], in: Record) => in.data, 32)(_.appended(_))
  }

  // 3. Create a composite transducer that operates on bytes; it should
  // decode the data to UTF8, split to lines, and group the lines into maps of lists
  // on their first letter, with up to 5 letters in every map.
  val transducer: ZSink[Any, Throwable, Byte, List[String], Map[Char, List[String]]] =
    (ZPipeline.utf8Decode >>> ZPipeline.splitLines)
      .map(List(_)) >>> ZSink.collectAllToMapN[Nothing, List[String], Char](5)(_.head.head)(
      _ ++ _
    )

  // 4. Batch records in this stream into groups of up to 50 records for as long as
  // the database writing operator is busy.
  val batchWhileBusy = recordStream(Schedule.fixed(500.millis).jittered(0.25, 1.5))
    .mapZIO(_ => Random.nextIntBetween(1, 5).flatMap(sleep => ZIO.sleep(sleep.seconds)))

  // 5. Perform adaptive batching in this stream: group the records in groups of
  // up to 50; as long as the resulting groups are under 40 records, the delay
  // between every batch emitted should increase by 50 millis.
  val adaptiveBatching = recordStream(Schedule.fixed(500.millis).jittered(0.25, 1.5))
    .aggregateAsyncWithin(ZSink.collectAllN[Record](40), Schedule.exponential(50.milliseconds))
    .mapZIO(_ => Random.nextIntBetween(1, 5).flatMap(sleep => ZIO.sleep(sleep.seconds)))

}
