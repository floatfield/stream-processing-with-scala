package streams.workshop

import zio._
import zio.stream._

object Chunks {
  // 1. Create a chunk of integers from an array.
  val intChunk: Chunk[Int] = Chunk.fromArray(Array.fill(10)(10))

  // 2. Fold a chunk of integers into its sum.
  val sum: Int = Chunk(1, 2, 3, 4, 5).sum

  // 3. Fold a chunk of integers into its sum and print the
  // partial sums as it is folded.
  val sum2: URIO[console.Console, Int] =
    Chunk(1, 2, 3).foldM(0)((cumSum, next) => console.putStrLn(cumSum.toString).as(cumSum + next))

  // 4. Copy the contents of a chunk to an array.
  val arr: Array[Int] = Chunk(1, 2, 3).toArray

  // 5. Incrementally build a chunk using a ChunkBuilder.
  val buildChunk: Chunk[Int] = (ChunkBuilder.make() ++= List.fill(10)(10)).result()
}

class PrimitiveBoxing {
  // Compare the bytecode resulting from each test here.
  def test0(): Int = {
    val vec = Vector(1, 2, 3)

    val i1 = vec(1)

    i1
  }

  def test1(): Int = {
    val chunk = Chunk(1, 2, 3)

    val i1 = chunk(1)

    i1
  }

  def test2(): Int = {
    val chunk = Chunk(1, 2, 3)

    val i1 = chunk.int(1)

    i1
  }
}

object ChunkedStreams {
  // 1. Create a stream from a chunk.
  val intStream: Stream[Nothing, Int] = ZStream.fromChunk(Chunk(1, 2, 3))

  // 2. Create a stream from multiple chunks.
  val intStream2: Stream[Nothing, Int] = ZStream.fromChunks(Chunk(1), Chunk(2, 3))

  // 3. Consume the chunks of a multiple chunk stream and print them out.
  val printChunks: URIO[console.Console, Unit] = intStream2.foreachChunk(chunk => console.putStrLn(chunk.toString()))

  // 4. Transform each chunk of integers to its sum and print it.
  val summedChunks: ZStream[console.Console, Nothing, Int] =
    ZStream
      .fromChunks(Chunk(1, 2), Chunk(3, 4), Chunk(5, 6))
      .mapChunks(chunk => Chunk(chunk.sum))
      .tap(chunk => console.putStrLn(chunk.toString()))

  // 5. Compare the chunking behavior of mapChunksM and mapM.
  val printedNumbers: ZStream[console.Console, Nothing, Int] =
    ZStream.fromChunks(Chunk(1, 2), Chunk(3, 4)).mapM(i => console.putStrLn(i.toString).as(i))

  val printedNumbersChunk: ZStream[console.Console, Nothing, Int] =
    ZStream.fromChunks(Chunk(1, 2), Chunk(3, 4)).mapChunksM(_.mapM(i => console.putStrLn(i.toString).as(i)))

  // 6. Compare the behavior of the following streams under errors.
  def faultyPredicate(i: Int): Task[Boolean] =
    if (i < 10) Task.succeed(i % 2 == 0)
    else Task.fail(new RuntimeException("Boom"))

  val filteredStream =
    ZStream
      .fromChunks(Chunk(1, 2, 3), Chunk(8, 9, 10, 11))
      .filterM(faultyPredicate)
      .tap(i => console.putStrLn(i.toString))

  val filteredChunkStream =
    ZStream
      .fromChunks(Chunk(1, 2, 3), Chunk(8, 9, 10, 11))
      .mapChunksM(_.filterM(faultyPredicate))
      .tap(i => console.putStrLn(i.toString))

  // 7. Re-chunk a singleton chunk stream into chunks of 4 elements.
  val rechunked: Stream[Nothing, Int] =
    ZStream.fromChunks(List.fill(8)(Chunk(1)): _*).chunkN(4)

  // 8. Build a stream of longs from 0 to Int.MaxValue + 3.
  val longs: ZStream[console.Console, Nothing, Long] =
    Stream
      .iterate(0L)(_ + 1L)
      .mapChunksM(ch => console.putStrLn(ch.length.toString).as(ch))
      .take(Int.MaxValue.toLong + 3L)

  // 9. Flatten this stream of chunks:
  val chunks: Stream[Nothing, Int] = ZStream(Chunk(1, 2, 3), Chunk(4, 5)).flattenChunks
}
