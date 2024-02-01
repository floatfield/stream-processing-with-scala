package streams.workshop

import zio._

import zio.stream._
import zio.Random

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.time.OffsetDateTime

object AccumulatingMaps {
  // 1. Compute a running sum of this infinite stream using mapAccum.
  val numbers = ZStream.iterate(0)(_ + 1).mapAccum(0)((sum, next) => (sum + next, sum + next))

  // 2. Use mapAccum to pattern match on the stream and group consecutive
  // rising numbers.

  // val risingNumbers = ZStream.repeatEffect(random.nextIntBetween(0, 21)) ?

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

  object Windowed {
    def fromLong(t: Long, duration: Duration, sum: Long): Windowed =
      Windowed(t, Instant.ofEpochMilli(t).plusMillis(duration.toMillis()).toEpochMilli(), sum)
  }

  val records =
    ZStream.repeatZIOWithSchedule(
      Random.nextLongBetween(0, 100).map(Record(_)),
      Schedule.fixed(5.seconds).jittered
    )

  def windowed[R, E, A](interval: Duration, records: ZStream[R, E, A])(f: A => Long): ZStream[R, E, Windowed] =
    ZStream
      .fromZIO(Clock.currentTime(ChronoUnit.MILLIS))
      .flatMap { time: Long =>
        records
          .mapAccumZIO(
            Windowed.fromLong(time, interval, 0L)
          ) { (window, record) =>
            for {
              t <- Clock.currentTime(ChronoUnit.MILLIS)
              newWindow = if (t < window.windowEnd)
                window.copy(sum = window.sum + f(record))
              else Windowed.fromLong(t, interval, f(record))
              producedValue = Some(window).filter(_ => t > window.windowEnd)
            } yield (newWindow, producedValue)
          }
          .collectSome
      }

  // TODO: implement chained windows

  // 4. Implement state save/restore for your windowing stream.
  trait StateStore {
    def saveState(windowId: Long, l: Long): Task[Unit]
    def loadState: Task[Map[Long, Long]]
  }

  def windowedPersistent[R, E, A](interval: Duration, records: ZStream[R, E, A], state: StateStore)(
    f: A => Long
  ): ZStream[R, E, Windowed] = ???
}

object Transduction extends ZIOAppDefault {
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
    def writeBatch(data: Map[String, Record]): Task[Unit]
  }
  object Database {
    def make: Database = data => ZIO.attempt(println(s"Writing ${data}")).delay(1.second)
  }

  val batcher   = ZSink.collectAllToMapN[Nothing, Record, String](n = 2)(_.key)((_, r2) => r2)
  val batcher50 = ZSink.collectAllToMapN[Nothing, Record, String](n = 50)(_.key)((_, r2) => r2)

  val records: ZStream[Any, Nothing, Map[String, Record]] =
    recordStream(Schedule.forever)
      .transduce(batcher)

  // 2. Group the `records` stream according to their cost - the value of data - with
  // up to 32 units in total in each group. Use ZTransducer.foldWeighted.
  val recordsWeighted = recordStream(Schedule.forever).transduce {
    ZSink.foldWeighted(Chunk[Record]())((_: Chunk[Record], in: Record) => in.data, 32)(_.appended(_))
  }

  // recordStream(Schedule.forever) ?

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

  def batchWhileBusy(database: Database): ZStream[Any, Throwable, Unit] =
    recordStream(Schedule.fixed(500.millis).jittered(0.25, 1.5))
      .aggregateAsync(batcher50)
      .mapZIO(database.writeBatch)

  batchWhileBusy(Database.make).runDrain
  // recordStream(Schedule.fixed(500.millis).jittered(0.25, 1.5)).mapM(???)
  def batcherN(n: Long) = ZSink.collectAllToMapN[Nothing, Record, String](n)(_.key)((_, r2) => r2)

  // 5. Perform adaptive batching in this stream: group the records in groups of
  // up to 50; as long as the resulting groups are under 40 records, the delay
  // between every batch emitted should increase by 50 millis.
  // 1 2 3 -> 40 ... 41 .... 42
  // A: Chunk[Record], B: ???, C: Chunk[Record]
  // def scheduleWith[R1 <: R, E1 >: E, B, C](
  //   schedule: => Schedule[R1, A, B]
  // )(f: A => C, g: B => C)

  val countSchedule: Schedule[Any, Option[Chunk[Record]], Int] =
    Schedule.fromFunction[Option[Chunk[Record]], Int](chunkOpt => chunkOpt.fold(0)(_.length))

  //f: (State, Out, Decision) => URIO[Env1, Either[Out2, (Out2, Interval)]]

  val foo = countSchedule
    .modifyDelay((len, dura) => if (len < 40) dura + 50.millis else dura)
    .reconsiderZIO((state, out, desicion) => Console.printLine((state, out, desicion)).orDie.as(Left(out)))

  val bar: Schedule.WithState[Duration, Any, Option[Chunk[Record]], Unit] =
    new Schedule[Any, Option[Chunk[Record]], Unit] {
      override type State = zio.Duration
      override final val initial: State = 0.millis

      override final def step(now: OffsetDateTime, in: Option[Chunk[Record]], state: State)(
        implicit
        trace: Trace
      ): ZIO[Any, Nothing, (State, Unit, Schedule.Decision)] = {
        val newState = if (in.fold[Int](0)(_.length) < 40) state + 10.millis else state
        ZIO.succeed((newState, (), Schedule.Decision.Continue(Schedule.Interval.after(now))))
      }
    }

  val adaptiveBatching = recordStream(Schedule.fixed(500.millis).jittered(0.25, 1.5))
    .aggregateAsyncWithin(sink = ZSink.collectAllN[Record](50), schedule = foo)
    .tap(ch => ZIO.sleep(10.seconds) *> Console.printLine(ch.length))

  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] = adaptiveBatching.runDrain
}
