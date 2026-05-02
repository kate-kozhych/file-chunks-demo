package downloader.api

import downloader.exception.ChunkDownloadException
import downloader.internal.http.ChunkDownloader
import downloader.internal.http.MetaFetcher
import downloader.internal.io.FileAssembler
import downloader.internal.strategy.AdaptiveChunkStrategy
import downloader.internal.strategy.ChunkStrategy
import kotlinx.coroutines.Dispatchers
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
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
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
            val bytesDownloaded = AtomicLong(0L)

            try {
                val probeRanges: List<LongRange>
                val remainingRanges: List<LongRange>

                if (adaptiveChunking) {
                    probeRanges = adaptiveStrategy.probeRanges(contentLength)
                    val probeDurations = downloadProbes(url, probeRanges, assembler, bytesDownloaded)
                    val adaptedSize =
                        adaptiveStrategy.computeAdaptedSize(
                            probeDurationsMs = probeDurations,
                            probeSizeBytes = probeRanges.first().let { it.last - it.first + 1 },
                        )
                    remainingRanges =
                        adaptiveStrategy.remainingRanges(
                            totalBytes = contentLength,
                            startOffset = probeRanges.last().last + 1,
                            adaptedChunkSize = adaptedSize,
                        )
                } else {
                    probeRanges = emptyList()
                    remainingRanges = ChunkStrategy(chunkSize).split(contentLength)
                }

                val totalChunks = probeRanges.size + remainingRanges.size
                send(DownloadEvent.Started(contentLength, totalChunks))

                coroutineScope {
                    val semaphore = Semaphore(parallelism)
                    val indexOffset = probeRanges.size

                    val jobs =
                        remainingRanges.mapIndexed { i, range ->
                            async {
                                semaphore.withPermit {
                                    downloadChunkWithRetry(
                                        url = url,
                                        range = range,
                                        index = indexOffset + i,
                                        assembler = assembler,
                                        bytesDownloaded = bytesDownloaded,
                                        contentLength = contentLength,
                                        totalRetries = totalRetries,
                                        onChunkDone = { idx, downloaded, percent ->
                                            send(DownloadEvent.ChunkCompleted(idx, downloaded, contentLength, percent))
                                        },
                                        onChunkRetry = { idx, attempt, reason ->
                                            send(DownloadEvent.ChunkRetrying(idx, attempt, reason))
                                        },
                                    )
                                }
                            }
                        }

                    jobs.awaitAll()
                }

                send(
                    DownloadEvent.Finished(
                        DownloadReport(
                            totalBytes = contentLength,
                            durationMs = System.currentTimeMillis() - startTime,
                            chunksCount = totalChunks,
                            retries = totalRetries.get(),
                            fallback = false,
                        ),
                    ),
                )
            } catch (e: Exception) {
                outputPath.deleteIfExists()
                throw e
            } finally {
                assembler.close()
            }
        }

    private suspend fun downloadProbes(
        url: String,
        probeRanges: List<LongRange>,
        assembler: FileAssembler,
        bytesDownloaded: AtomicLong,
    ): List<Long> =
        coroutineScope {
            probeRanges.map { range ->
                async {
                    val before = System.currentTimeMillis()
                    val result = chunkDownloader.download(url, range, -1)
                    assembler.write(range.first, result.bytes)
                    bytesDownloaded.addAndGet(result.bytes.size.toLong())
                    System.currentTimeMillis() - before
                }
            }.awaitAll()
        }

    private suspend fun downloadChunkWithRetry(
        url: String,
        range: LongRange,
        index: Int,
        assembler: FileAssembler,
        bytesDownloaded: AtomicLong,
        contentLength: Long,
        totalRetries: AtomicInteger,
        onChunkDone: suspend (index: Int, downloaded: Long, percent: Int) -> Unit,
        onChunkRetry: suspend (index: Int, attempt: Int, reason: String) -> Unit,
    ) {
        var attempts = 0
        var lastError: Exception? = null

        while (attempts <= retryPolicy.maxAttempts) {
            try {
                val result = chunkDownloader.download(url, range, index)
                assembler.write(range.first, result.bytes)
                val downloaded = bytesDownloaded.addAndGet(result.bytes.size.toLong())
                val percent = if (contentLength > 0) (downloaded * 100 / contentLength).toInt() else 0
                onChunkDone(index, downloaded, percent)
                return
            } catch (e: Exception) {
                lastError = e
                if (attempts < retryPolicy.maxAttempts) {
                    totalRetries.incrementAndGet()
                    onChunkRetry(index, attempts, e.message ?: "unknown")
                    delay(retryPolicy.delayMs(attempts))
                }
                attempts++
            }
        }

        throw ChunkDownloadException("Chunk $index failed", lastError)
    }

    private suspend fun fallbackDownload(
        url: String,
        outputPath: Path,
        startTime: Long,
    ): DownloadEvent =
        withContext(Dispatchers.IO) {
            val request = okhttp3.Request.Builder().url(url).build()
            client.newCall(request).execute().use { response ->
                val bytes = response.body?.bytes() ?: throw Exception("Empty response")
                outputPath.toFile().writeBytes(bytes)
                DownloadEvent.Finished(
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
