package streams.workshop

import zio._
import zio.stream._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import java.util.zip.GZIPOutputStream
import java.nio.file.Path
import java.io.FileOutputStream

object Sinks {
  // 1. Extract the first element of this stream using runHead.
  val head = ZStream.unwrap(Random.nextInt.map(ZStream.range(0, _))).runHead

  // 2. Extract the last element of this stream using runLast.
  val last = ZStream.unwrap(Random.nextInt.map(ZStream.range(0, _))).runLast

  // 3. Parse the CSV file at the root of the repository into its header line
  // and a stream that represents the rest of the lines. Use `ZStream#peel`.
  val peelstream = ZStream
    .fromPath(Path.of("DDOG.csv"))
    .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
    .peel(ZSink.head[String])

  // 4. Transduce the bytes read from a file into lines and print them out;
  // sum the amount of bytes that were read from the file and print that out
  // when the stream ends.
  // Use: ZStream#tap for writing, ZSink.count+contramap for summing
  val sumBytesSink = ZSink.sum[Int].contramap((in: String) => in.getBytes().length)
  val sumBytes = ZStream
    .fromPath(Path.of("DDOG.csv"))
    .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
    .tap(Console.printLine(_))
    .run(sumBytesSink)

  // 5. Use ZSink.managed to wrap a Kafka producer and write every line
  // from a file to a topic.
  def mkProducer: ZIO[Scope, Throwable, KafkaProducer[String, String]] = {
    val props = new java.util.Properties
    props.put("bootstrap.server", "localhost:9092")
    ZIO.acquireRelease(
      ZIO.attempt(new KafkaProducer(props, new StringSerializer, new StringSerializer))
    )(producer => ZIO.attempt(producer.close()).orDie)
  }

  def toProducerRecord(topic: String, value: String): ProducerRecord[String, String] =
    new ProducerRecord[String, String](topic, value)

  def sendToTopic(topic: String): ZSink[Scope, Throwable, String, Nothing, Unit] =
    ZSink
      .fromZIO(mkProducer)
      .flatMap(producer =>
        ZSink.foreach(line =>
          ZIO.fromFutureJava(
            producer.send(toProducerRecord(topic, line))
          )
        )
      )

  def pipeFileToTopic(file: String, topic: String): Task[Unit] =
    ZIO.scoped {
      ZStream
        .fromPath(Path.of(file))
        .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
        .run(sendToTopic(topic))
    }

  // 6. Combine the sink from `sumBytes` and the sink from `pipeFileToTopic` into
  // a single sink that both writes to Kafka and counts how many bytes were written
  // using `zipParRight`.
  def superSink(topic: String) = sumBytesSink &> sendToTopic(topic)
  // 7. Use ZSink.fold to sample 10 random elements from the stream, and then switch
  // to ZSink.collectAllToSet to gather the rest of the elements of the stream.
  val sequencedSinks = ZStream.repeatZIO(Random.shuffle(List("a", "b", "c", "d")).map(_.head))
  val s: ZSink[Any, Nothing, String, Nothing, (List[String], Chunk[String])] = ZSink
    .foldZIO(List.empty[String])(_.length < 10)((seq, el: String) =>
      Random.nextBoolean.map(bool => if (bool) el +: seq else seq)
    )
    .collectLeftover

  sequencedSinks.run(s)

  // 8. Use ZSink.fromOutputStream, GZIPOutputStream and ZStream.fromFile to compress a file.
  def compressFile(filename: String) =
    ZStream.fromFileName(filename).run(ZSink.fromOutputStream(new GZIPOutputStream(new FileOutputStream(filename))))
}
