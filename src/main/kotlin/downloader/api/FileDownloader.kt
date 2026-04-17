package downloader.api

import downloader.exception.ChunkDownloadException
import downloader.internal.http.ChunkDownloader
import downloader.internal.http.MetaFetcher
import downloader.internal.io.FileAssembler
import downloader.internal.strategy.AdaptiveChunkStrategy
import downloader.internal.strategy.ChunkStrategy
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import okhttp3.OkHttpClient
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.deleteIfExists

class FileDownloader private constructor(
    private val parallelism: Int,
    private val chunkSize: Long,
    private val retryPolicy: RetryPolicy,
    private val client: OkHttpClient,
    private val adaptiveChunking: Boolean,
) {
    private val metaFetcher = MetaFetcher(client)
    private val chunkDownloader = ChunkDownloader(client)
    private val adaptiveStrategy = AdaptiveChunkStrategy(initialChunkSize = chunkSize)

    fun download(
        url: String,
        outputPath: Path,
    ): DownloadReport =
        runBlocking {
            downloadWithProgress(url, outputPath)
                .filterIsInstance<DownloadEvent.Finished>()
                .first()
                .report
        }

    fun downloadWithProgress(
        url: String,
        outputPath: Path,
    ): Flow<DownloadEvent> =
        channelFlow {
            val startTime = System.currentTimeMillis()
            val meta = metaFetcher.fetch(url)

            val contentLength = meta.contentLength ?: 0L
            if (!meta.acceptsRanges || contentLength <= 0L) {
                send(fallbackDownload(url, outputPath, startTime))
                return@channelFlow
            }

            val assembler = FileAssembler(outputPath)
            val totalRetries = AtomicInteger(0)
            val bytesDownloaded = AtomicInteger(0)

            val ranges =
                if (adaptiveChunking) {
                    buildAdaptiveRanges(url, contentLength, assembler, bytesDownloaded)
                } else {
                    ChunkStrategy(chunkSize).split(contentLength)
                }

            send(DownloadEvent.Started(contentLength, ranges.size))

            try {
                coroutineScope {
                    val semaphore = Semaphore(parallelism)

                    val jobs =
                        ranges.mapIndexed { index, range ->
                            async {
                                semaphore.withPermit {
                                    var attempts = 0
                                    var lastError: Exception? = null

                                    while (attempts <= retryPolicy.maxAttempts) {
                                        try {
                                            val result =
                                                chunkDownloader.download(
                                                    url,
                                                    range,
                                                    index,
                                                )
                                            assembler.write(range.first, result.bytes)

                                            val downloaded = bytesDownloaded.addAndGet(result.bytes.size).toLong()
                                            val percent = (downloaded * 100 / contentLength).toInt()
                                            send(DownloadEvent.ChunkCompleted(index, downloaded, contentLength, percent))
                                            break
                                        } catch (e: Exception) {
                                            lastError = e
                                            if (attempts < retryPolicy.maxAttempts) {
                                                totalRetries.incrementAndGet()
                                                send(DownloadEvent.ChunkRetrying(index, attempts, e.message ?: "unknown"))
                                                delay(retryPolicy.delayMs(attempts))
                                            }
                                            attempts++
                                        }
                                    }

                                    if (lastError != null && attempts > retryPolicy.maxAttempts) {
                                        throw ChunkDownloadException("Chunk $index failed", lastError)
                                    }
                                }
                            }
                        }

                    jobs.awaitAll()
                }
            } catch (e: Exception) {
                outputPath.deleteIfExists()
                throw e
            } finally {
                assembler.close()
            }

            val report =
                DownloadReport(
                    totalBytes = contentLength,
                    durationMs = System.currentTimeMillis() - startTime,
                    chunksCount = ranges.size,
                    retries = totalRetries.get(),
                    fallback = false,
                )
            send(DownloadEvent.Finished(report))
        }

    private suspend fun buildAdaptiveRanges(
        url: String,
        totalBytes: Long,
        assembler: FileAssembler,
        bytesDownloaded: AtomicInteger,
    ): List<LongRange> {
        val probeRanges = adaptiveStrategy.probeRanges(totalBytes)

        val probeDurations =
            coroutineScope {
                probeRanges.mapIndexed { index, range ->
                    async {
                        val result = chunkDownloader.download(url, range, index)
                        assembler.write(range.first, result.bytes)
                        bytesDownloaded.addAndGet(result.bytes.size)
                        result.durationMs
                    }
                }.awaitAll()
            }

        val adaptedSize =
            adaptiveStrategy.computeAdaptedSize(
                probeDurationsMs = probeDurations,
                probeSizeBytes = probeRanges.first().let { it.last - it.first + 1 },
            )

        val probeEnd = probeRanges.last().last + 1
        val remainingRanges = adaptiveStrategy.remainingRanges(totalBytes, probeEnd, adaptedSize)

        return probeRanges + remainingRanges
    }

    private fun fallbackDownload(
        url: String,
        outputPath: Path,
        startTime: Long,
    ): DownloadEvent {
        val request = okhttp3.Request.Builder().url(url).build()
        client.newCall(request).execute().use { response ->
            val bytes = response.body?.bytes() ?: throw Exception("Empty response")
            outputPath.toFile().writeBytes(bytes)
            return DownloadEvent.Finished(
                DownloadReport(
                    totalBytes = bytes.size.toLong(),
                    durationMs = System.currentTimeMillis() - startTime,
                    chunksCount = 1,
                    retries = 0,
                    fallback = true,
                ),
            )
        }
    }

    companion object {
        fun builder() = Builder()
    }

    class Builder {
        private var parallelism: Int = 4
        private var chunkSize: Long = 512 * 1024
        private var retryPolicy: RetryPolicy = RetryPolicy.fixed(3)
        private var client: OkHttpClient = OkHttpClient()
        private var adaptiveChunking: Boolean = false

        fun parallelism(value: Int) = apply { parallelism = value }

        fun chunkSize(bytes: Long) = apply { chunkSize = bytes }

        fun retryPolicy(policy: RetryPolicy) = apply { retryPolicy = policy }

        fun httpClient(client: OkHttpClient) = apply { this.client = client }

        fun adaptiveChunking(enabled: Boolean) = apply { adaptiveChunking = enabled }

        fun build() = FileDownloader(parallelism, chunkSize, retryPolicy, client, adaptiveChunking)
    }
}
