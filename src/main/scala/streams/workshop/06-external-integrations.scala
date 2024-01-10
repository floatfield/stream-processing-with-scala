package streams.workshop

import java.util.concurrent.atomic.{ AtomicBoolean, AtomicReference }
import java.sql.{ Connection, DriverManager, ResultSet }
import java.{ util => ju }

import zio._

import zio.stream._
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.S3Object
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import java.net.URI

object ExternalSources {
  // 1. Refactor this function, which drains a java.sql.ResultSet,
  // to use ZStream and ZManaged.
  // Type: Unbounded, stateless iteration
  def readRows(url: String, connProps: ju.Properties, sql: String): Chunk[String] = {
    var conn: Connection = null
    try {
      conn = DriverManager.getConnection(url, connProps)

      var resultSet: ResultSet = null
      try {
        val st = conn.createStatement()
        st.setFetchSize(5)
        resultSet = st.executeQuery(sql)

        val buf = mutable.ArrayBuilder.make[String]
        while (resultSet.next())
          buf += resultSet.getString(0)

        Chunk.fromArray(buf.result())
      } finally {
        if (resultSet ne null)
          resultSet.close()
      }

    } finally {
      if (conn ne null)
        conn.close()
    }
  }

  def readRowsZIO(
    url: String,
    connProps: ju.Properties,
    sql: String
  ): ZStream[Any, Throwable, String] =
    for {
      connection <- ZStream
                     .acquireReleaseWith(
                       ZIO.attempt(DriverManager.getConnection(url, connProps))
                     )(conn => ZIO.attempt(conn.close()).orDie)
      statement <- ZStream.fromZIO(ZIO.attempt(connection.createStatement()))
      _         <- ZStream.fromZIO(ZIO.attempt(statement.setFetchSize(5)))
      resultSet <- ZStream.acquireReleaseWith(ZIO.attempt(statement.executeQuery(sql)))(resultSet =>
                    ZIO.attempt(resultSet.close()).orDie
                  )
      result <- ZStream.repeatZIOOption {
                 val a = ZIO.fail(None).whenZIO(ZIO.attempt(resultSet.next()).mapError(Option(_)))
                 val b = ZIO
                   .attempt(resultSet.getString(0))
                   .mapError(Option(_))
                 a *> b
               }
    } yield result

  // 2. Convert this function, which polls a Kafka consumer, to use ZStream and
  // ZManaged.
  // Type: Unbounded, stateless iteration
  def pollConsumer(topic: String)(f: ConsumerRecord[String, String] => Unit): Unit = {
    val props = new ju.Properties
    props.put("bootstrap.server", "localhost:9092")
    props.put("group.id", "streams")
    props.put("auto.offset.reset", "earliest")
    var consumer: KafkaConsumer[String, String] = null

    try {
      consumer = new KafkaConsumer[String, String](props, new StringDeserializer, new StringDeserializer)
      consumer.subscribe(List(topic).asJava)

      while (true) consumer.poll(50.millis.asJava).forEach(f(_))
    } finally {
      if (consumer ne null)
        consumer.close()
    }
  }

  private def consumerStream(
    consumer: KafkaConsumer[String, String]
  ): ZStream[Any, Throwable, ConsumerRecord[String, String]] =
    ZStream.repeatZIOChunk(ZIO.attempt(consumer.poll(50.millis.asJava)).map(Chunk.fromJavaIterable(_)))

  def pollConsumerZIO(topic: String)(f: ConsumerRecord[String, String] => Unit): RIO[Scope, Unit] = {
    val props = new ju.Properties
    (for {
      _ <- ZIO.attempt(props.put("bootstrap.server", "localhost:9092")).orDie
      _ <- ZIO.attempt(props.put("group.id", "streams")).orDie
      _ <- ZIO.attempt(props.put("auto.offset.reset", "earliest")).orDie
    } yield ()) *>
      (
        ZIO
          .acquireRelease(
            ZIO.succeed(new KafkaConsumer[String, String](props, new StringDeserializer, new StringDeserializer))
          )(consumer => ZIO.attempt(consumer.close()).orDie)
        )
        .tap(consumer => ZIO.attempt(consumer.subscribe(List(topic).asJava)))
        .flatMap(consumerStream(_).mapZIO(record => ZIO.attempt(f(record))).runDrain)

  }

  // 3. Convert this function, which enumerates keys in an S3 bucket, to use ZStream and
  // ZManaged. Bonus points for using S3AsyncClient instead.
  // Type: Unbounded, stateful iteration
  def listFiles(bucket: String, prefix: String): Chunk[S3Object] = {
    val client = S3Client
      .builder()
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("minio", "minio123")))
      .endpointOverride(URI.create("http://localhost:9000"))
      .build()

    def listFilesToken(acc: Chunk[S3Object], token: Option[String]): Chunk[S3Object] = {
      val reqBuilder = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix)
      token.foreach(reqBuilder.continuationToken)
      val req  = reqBuilder.build()
      val resp = client.listObjectsV2(req)
      val data = Chunk.fromIterable(resp.contents().asScala)

      if (resp.isTruncated()) listFilesToken(acc ++ data, Some(resp.nextContinuationToken()))
      else acc ++ data
    }

    listFilesToken(Chunk.empty, None)
  }

  def listFilesZIO(bucket: String, prefix: String): Task[Chunk[S3Object]] = {

    def listFilesChunk(bucket: String, prefix: String, client: S3Client)(
      token: Option[String]
    ): Task[(Chunk[S3Object], Option[Option[String]])] =
      for {
        reqBuilder  <- ZIO.attempt(ListObjectsV2Request.builder().bucket(bucket).prefix(prefix))
        builder     = token.fold(reqBuilder)(token => reqBuilder.continuationToken(token))
        req         <- ZIO.succeed(builder.build())
        resp        <- ZIO.attemptBlocking(client.listObjectsV2(req))
        data        = Chunk.fromIterable(resp.contents().asScala)
        cond        = resp.isTruncated()
        newTokenOpt = if (cond) Some(Some(resp.nextContinuationToken())) else None
      } yield (data, newTokenOpt)

    for {
      client <- ZIO.attempt(
                 S3Client
                   .builder()
                   .credentialsProvider(
                     StaticCredentialsProvider.create(AwsBasicCredentials.create("minio", "minio123"))
                   )
                   .endpointOverride(URI.create("http://localhost:9000"))
                   .build()
               )

      s3objects <- ZStream.paginateChunkZIO(Option.empty[String])(listFilesChunk(bucket, prefix, client)).runCollect
    } yield s3objects
  }

  // 4. Convert this push-based mechanism into a stream. Hint: you'll need a queue.
  // Type: callback-based iteration
  case class Message(body: String)

  trait Subscriber {
    def onError(err: Throwable): Unit
    def onMessage(msg: Message): Unit
    def onShutdown(): Unit
  }

  trait RabbitMQ {
    def register(subscriber: Subscriber): Unit
    def shutdown(): Unit
  }

  object RabbitMQ {
    def make: RabbitMQ = new RabbitMQ {
      val subs: AtomicReference[List[Subscriber]] = new AtomicReference(Nil)
      val shouldStop: AtomicBoolean               = new AtomicBoolean(false)
      val thread: Thread = new Thread {
        override def run(): Unit = {
          while (!shouldStop.get()) {
            if (scala.util.Random.nextInt(11) < 7)
              subs.get().foreach(_.onMessage(Message(s"Hello ${java.lang.System.currentTimeMillis}")))
            else
              subs.get().foreach(_.onError(new RuntimeException("Boom!")))

            Thread.sleep(1000)
          }

          subs.get().foreach(_.onShutdown())
        }
      }

      thread.run()

      def register(sub: Subscriber): Unit = {
        subs.updateAndGet(sub :: _)
        ()
      }
      def shutdown(): Unit = shouldStop.set(true)
    }
  }

  def messageStream(rabbit: RabbitMQ): ZStream[Any, Throwable, Message] =
    ZStream.async { emit =>
      def subscriber = new Subscriber {
        def onError(err: Throwable): Unit = emit(ZIO.fail(Some(err)))
        def onMessage(msg: Message): Unit = emit(ZIO.succeed(Chunk(msg)))
        def onShutdown(): Unit            = emit(ZIO.fail(None))
      }

      rabbit.register(subscriber)
    }

  // 5. Convert this Reactive Streams-based publisher to a ZStream.
  val publisher =
    ZStream
      .acquireReleaseWith(ZIO.attempt(RabbitMQ.make))(rabbit => ZIO.succeed(rabbit.shutdown()))
      .flatMap(messageStream)

  trait Rabbit {
    def subscribe: ZStream[Any, Throwable, Message]
    def shutdown: UIO[Unit]
  }

  object Rabbit {

    def make: Rabbit = new Rabbit {

      def subscribe: ZStream[Any, Throwable, Message] = ???
      def shutdown: UIO[Unit]                         = ???
    }
  }

  /* for {
    rabbit <- Rabbit.make
    fiber  <- ZIO.forkAll(List.fill(5)(rabbit.subscribe.foreach(m => printLine(m))))
    _      <- ZIO.sleep(5.seconds)
    _      <- fiber.interrupt
  } yeild() */

}
