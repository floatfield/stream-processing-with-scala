package streams.workshop

import zio._
import zio.stream._
import java.nio.file.Path
import java.nio.file.FileSystems
import java.nio.file.StandardWatchEventKinds
import scala.jdk.CollectionConverters._
import java.nio.file.WatchEvent
import org.postgresql.jdbc.PgDatabaseMetaData

object Resources {
  // Resource management is an important part of stream processing. Resources can be
  // opened and closed throughout the stream's lifecycle, and most importantly,
  // need to be kept open for precisely as long as they are required for processing
  // the stream's data.

  class DatabaseClient(clientId: String) {
    def readRow: URIO[Any, String]       = Random.nextString(5).map(str => s"${clientId}-${str}")
    def writeRow(str: String): UIO[Unit] = ZIO.succeed(println(s"Writing ${str}"))
    def close: UIO[Unit]                 = ZIO.succeed(println(s"Closing $clientId"))
  }
  object DatabaseClient {
    def make(clientId: String): Task[DatabaseClient] =
      ZIO.succeed(println(s"Opening $clientId")).as(new DatabaseClient(clientId))
  }

  // это самоделка
  private def clientDb(clientName: String): ZStream[Any, Throwable, DatabaseClient] =
    ZStream.acquireReleaseWith(DatabaseClient.make(clientName))(dbClient => dbClient.close)

  // 1. Create a stream that allocates the database client, reads 5 rows, writes
  // them back to the client and ends.
  val fiveRows(client: ZStream[Any, Throwable, DatabaseClient]): ZStream[Any, Throwable, String] = client
    .flatMap(db => ZStream.repeatZIO(db.readRow).take(5).tap(db.writeRow))

  // 2. Create a stream that reads 5 rows from 3 different database clients, and writes
  // them to a fourth (separate!) client, closing each reading client after finishing reading.
  val fifteenRows: ZStream[Any, Throwable, Unit] = {
    val readingClients = ZStream.fromIterable(Range(1, 4)).flatMap(id => clientDb(s"reading client $id"))
    val writingClient  = clientDb("IBM-6000")
    for {
      write <- writingClient
      sol   <- readingClients.mapZIOPar(3)(read => ZStream.repeatZIO(read.readRow).take(5).map(write.writeRow))
    } yield sol
  }

  def dbClient(clientId: String): ZIO[Scope, Throwable, DatabaseClient] =
    ZIO.acquireRelease(DatabaseClient.make(clientId))(_.close)

  def writeToClient(data: Chunk[String], writeClient: DatabaseClient): Task[Unit] =
    ZStream.fromChunk(data).foreach(writeClient.writeRow)

  def writeToClients(
    readClient: DatabaseClient,
    writeClients: List[DatabaseClient],
    rowsToRead: Int,
    rowsToWriteToClient: Int
  ): Task[Unit] =
    readClient.readRow.map(
      data => ZIO.(
        
      )
    )

  def mkStream(
    readClientIds: List[String],
    writeClientIds: List[String],
    rowsToRead: Int,
    rowsToWriteToClient: Int
  ): ZStream[Scope, Throwable, Unit] = {}
  // 3. Read 25 rows from 3 different database clients, and write the rows to 5 additional
  // database clients - 5 rows each. Hint: use ZManaged.scope.
  val scopes: ZStream[Random, Throwable, String] = ZStream.scoped{}
}

object FileIO {
  // 1. Implement the following combinator for reading bytes from a file using
  // java.io.FileInputStream.
  def readFileBytes(path: String): ZStream[???, ???, Byte] = ???

  // 2. Implement the following combinator for reading characters from a file using
  // java.io.FileReader.
  def readFileChars(path: String): ZStream[???, ???, Char] = ???

  // 3. Recursively enumerate all files in a directory.
  def listFilesRecursive(path: String): ZStream[???, ???, Path] = ???

  // 4. Read data from all files in a directory tree.
  def readAllFiles(path: String): ZStream[???, ???, Char] = ???

  // 5. Monitor a directory for new files using Java's WatchService.
  // Imperative example:
  def monitor(path: Path): Unit = {
    val watcher = FileSystems.getDefault().newWatchService()
    path.register(watcher, StandardWatchEventKinds.ENTRY_CREATE)
    var cont = true

    while (cont) {
      val key = watcher.take()

      for (watchEvent <- key.pollEvents().asScala) {
        watchEvent.kind match {
          case StandardWatchEventKinds.ENTRY_CREATE =>
            val pathEv   = watchEvent.asInstanceOf[WatchEvent[Path]]
            val filename = pathEv.context()

            println(s"${filename} created")
        }
      }

      cont = key.reset()
    }
  }

  def monitorFileCreation(path: String): ZStream[???, ???, Path] = ???

  // 6. Write a stream that synchronizes directories.
  def synchronize(source: String, dest: String): ??? = ???
}

object SocketIO {
  // 1. Print the first 2048 characters of the URL.
  def readUrl(url: String): ZStream[???, ???, Char] = ???

  // 2. Create an echo server with ZStream.fromSocketServer.
  val server = ZStream.fromSocketServer(???, ???)

  // 3. Use `ZStream#toInputStream` and `java.io.InputStreamReader` to decode a
  // stream of bytes from a file to a string.
  val data = ZStream.fromFile(???) ?

  // 4. Integrate GZIP decoding using GZIPInputStream, ZStream#toInputStream
  // and ZStream.fromInputStream.
  val gzipDecodingServer = ZStream.fromSocketServer(???, ???)
}
