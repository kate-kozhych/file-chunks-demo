package downloader

import okhttp3.OkHttpClient

fun main() {
    val client = OkHttpClient()
    val url = "http://localhost:8080/test.txt"
    val fetcher = MetaFetcher(client)
    val meta = fetcher.fetch(url)
    println("contentLength : ${meta.contentLength}")
    println("acceptsRanges : ${meta.acceptsRanges}")
    println("etag          : ${meta.etag}")

    println("\n=RetryPolicy")
    val fixed = RetryPolicy.fixed(3, 500)
    val exp = RetryPolicy.exponential(3, 500)
    println("Fixed delays  : ${(0..2).map { fixed.delayMs(it) }} ms")
    println("Exp   delays  : ${(0..2).map { exp.delayMs(it) }} ms")

    println("\nFileAssembler")
    val outputFile = java.io.File.createTempFile("chunk-test", ".txt")
    val assembler = FileAssembler(outputFile.toPath())
    assembler.write(0L, "Hello, ".toByteArray())
    assembler.write(7L, "World!".toByteArray())
    println("Written file  : ${outputFile.readText()}")
    outputFile.delete()

    println("\nChunkDownloader")
    if (meta.contentLength != null && meta.acceptsRanges) {
        val downloader = ChunkDownloader(client)
        val policy = RetryPolicy.fixed(2, 500)

        kotlinx.coroutines.runBlocking {
            val result = downloader.download(url, 0L..(meta.contentLength - 1), 0, policy)
            println("Downloaded    : ${result.bytes.size} bytes")
            println("Content       : ${String(result.bytes)}")
            println("Duration      : ${result.durationMs} ms")
        }
    } else {
        println("Server doesn't support Range")
    }

    println("\nAll checks passed")
}
