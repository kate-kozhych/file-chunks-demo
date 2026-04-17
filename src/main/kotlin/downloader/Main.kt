package downloader

import downloader.api.DownloadEvent
import downloader.api.FileDownloader
import downloader.api.RetryPolicy

fun main() {
    val url = "http://localhost:8080/test.txt"
    val output = java.nio.file.Path.of("downloaded.txt")

    val downloader =
        FileDownloader.builder()
            .parallelism(4)
            .chunkSize(20)
            .retryPolicy(RetryPolicy.exponential(3))
            .adaptiveChunking(true)
            .build()

    kotlinx.coroutines.runBlocking {
        downloader.downloadWithProgress(url, output).collect { event ->
            when (event) {
                is DownloadEvent.Started ->
                    println("Started: ${event.totalBytes} bytes, ${event.chunks} chunks")
                is DownloadEvent.ChunkCompleted ->
                    println("Chunk ${event.index} done — ${event.percent}%")
                is DownloadEvent.ChunkRetrying ->
                    println("Chunk ${event.index} retrying (attempt ${event.attempt})")
                is DownloadEvent.Finished ->
                    println("Done: ${event.report}")
            }
        }
    }

    println("\nFile content: ${output.toFile().readText()}")
    output.toFile().delete()
}
