package downloader.api

import downloader.exception.ChunkDownloadException
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import okio.Buffer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class FileDownloaderTest {
    private lateinit var server: MockWebServer
    private lateinit var outputPath: Path
    private lateinit var downloader: FileDownloader

    @BeforeEach
    fun setup() {
        server = MockWebServer()
        server.start()
        outputPath = Files.createTempFile("download-test", ".bin").also { Files.delete(it) }
        downloader =
            FileDownloader.builder()
                .parallelism(4)
                .chunkSize(20)
                .retryPolicy(RetryPolicy.fixed(2, 0L))
                .build()
    }

    @AfterEach
    fun teardown() {
        server.shutdown()
        Files.deleteIfExists(outputPath)
    }

    @Test
    fun `happy path`() {
        val bytes = "I am test content for the downloader!".toByteArray()
        server.dispatcher = rangeDispatcher(bytes)

        val report = downloader.download(serverUrl(), outputPath)

        assertContentEquals(bytes, Files.readAllBytes(outputPath))
        assertFalse(report.fallback)
        assertEquals(bytes.size.toLong(), report.totalBytes)
    }

    @Test
    fun `binary integrity`() {
        val bytes = Random.nextBytes(200)
        server.dispatcher = rangeDispatcher(bytes)

        downloader.download(serverUrl(), outputPath)

        assertEquals(sha256(bytes), sha256(Files.readAllBytes(outputPath)))
    }

    @Test
    fun `out of order chunks`() {
        val bytes = Random.nextBytes(100)

        server.dispatcher =
            object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    if (request.method == "HEAD") return headResponse(bytes)
                    val range = request.getHeader("Range") ?: return MockResponse().setResponseCode(400)
                    val response = chunkResponse(bytes, request)
                    return when (range) {
                        "bytes=0-19" -> response.setBodyDelay(300, java.util.concurrent.TimeUnit.MILLISECONDS)
                        "bytes=20-39" -> response.setBodyDelay(50, java.util.concurrent.TimeUnit.MILLISECONDS)
                        else -> response
                    }
                }
            }

        downloader.download(serverUrl(), outputPath)

        assertContentEquals(bytes, Files.readAllBytes(outputPath))
    }

    @Test
    fun `range header`() {
        val bytes = "0123456789abcdefghij".toByteArray()
        server.dispatcher = rangeDispatcher(bytes)

        downloader.download(serverUrl(), outputPath)

        server.takeRequest()
        val chunkRequest = server.takeRequest()
        assertEquals("bytes=0-19", chunkRequest.getHeader("Range"))
    }

    @Test
    fun `small file`() {
        val bytes = "tiny".toByteArray()
        server.dispatcher = rangeDispatcher(bytes)

        val report = downloader.download(serverUrl(), outputPath)

        assertEquals(1, report.chunksCount)
        assertContentEquals(bytes, Files.readAllBytes(outputPath))
    }

    @Test
    fun `exact size`() {
        val bytes = ByteArray(40) { it.toByte() }
        server.dispatcher = rangeDispatcher(bytes)

        val report = downloader.download(serverUrl(), outputPath)

        assertEquals(2, report.chunksCount)
        assertContentEquals(bytes, Files.readAllBytes(outputPath))
    }

    @Test
    fun `retry on failure`() {
        val bytes = "I am test content!!!".toByteArray()
        var firstChunkRequest = true

        server.dispatcher =
            object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    if (request.method == "HEAD") return headResponse(bytes)
                    if (firstChunkRequest) {
                        firstChunkRequest = false
                        return MockResponse().setResponseCode(500)
                    }
                    return chunkResponse(bytes, request)
                }
            }

        val report = downloader.download(serverUrl(), outputPath)

        assertEquals(1, report.retries)
        assertContentEquals(bytes, Files.readAllBytes(outputPath))
    }

    @Test
    fun `retries exhausted`() {
        val bytes = "fail".toByteArray()

        server.dispatcher =
            object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    if (request.method == "HEAD") return headResponse(bytes)
                    return MockResponse().setResponseCode(500)
                }
            }

        assertFailsWith<ChunkDownloadException> {
            downloader.download(serverUrl(), outputPath)
        }
    }

    @Test
    fun `fallback`() {
        val bytes = "fallback content".toByteArray()

        server.dispatcher =
            object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    if (request.method == "HEAD") {
                        return MockResponse()
                            .setResponseCode(200)
                            .setHeader("Content-Length", bytes.size.toString())
                    }
                    return MockResponse()
                        .setResponseCode(200)
                        .setBody(Buffer().write(bytes))
                }
            }

        val report = downloader.download(serverUrl(), outputPath)

        assertTrue(report.fallback)
        assertEquals(1, report.chunksCount)
        assertContentEquals(bytes, Files.readAllBytes(outputPath))
    }

    @Test
    fun `unknown Content-Length`() {
        val bytes = "no length".toByteArray()
        var headServed = false

        server.dispatcher =
            object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    if (request.method == "HEAD" && !headServed) {
                        headServed = true
                        return MockResponse()
                            .setResponseCode(200)
                            .setHeader("Accept-Ranges", "bytes")
                    }
                    return MockResponse()
                        .setResponseCode(200)
                        .setBody(Buffer().write(bytes))
                }
            }

        val report = downloader.download(serverUrl(), outputPath)

        assertTrue(report.fallback)
        assertContentEquals(bytes, Files.readAllBytes(outputPath))
    }

    @Test
    fun `head 405`() {
        val bytes = "probe test content!!".toByteArray()
        var headAttempted = false

        server.dispatcher =
            object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    if (request.method == "HEAD" && !headAttempted) {
                        headAttempted = true
                        return MockResponse().setResponseCode(405)
                    }
                    return chunkResponse(bytes, request)
                }
            }

        val report = downloader.download(serverUrl(), outputPath)

        assertFalse(report.fallback)
        assertContentEquals(bytes, Files.readAllBytes(outputPath))
    }

    @Test
    fun `output file deleted`() {
        val bytes = "cleanup".toByteArray()

        server.dispatcher =
            object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    if (request.method == "HEAD") return headResponse(bytes)
                    return MockResponse().setResponseCode(500)
                }
            }

        assertFailsWith<ChunkDownloadException> {
            downloader.download(serverUrl(), outputPath)
        }

        assertFalse(Files.exists(outputPath), "Output file must be deleted after failure!")
    }

    @Test
    fun `progress flow`() {
        val bytes = "flow test 20 bytes!!".toByteArray()
        server.dispatcher = rangeDispatcher(bytes)

        val events = mutableListOf<DownloadEvent>()
        kotlinx.coroutines.runBlocking {
            downloader.downloadWithProgress(serverUrl(), outputPath).collect { events.add(it) }
        }

        assertTrue(events.first() is DownloadEvent.Started)
        assertTrue(events.last() is DownloadEvent.Finished)
        assertEquals(100, events.filterIsInstance<DownloadEvent.ChunkCompleted>().last().percent)
    }

    @Test
    fun `semaphore respects parallelism limit`() {
        val bytes = ByteArray(50) { it.toByte() }
        val concurrent = java.util.concurrent.atomic.AtomicInteger(0)
        val maxConcurrent = java.util.concurrent.atomic.AtomicInteger(0)

        val limitedDownloader =
            FileDownloader.builder()
                .parallelism(2)
                .chunkSize(10)
                .retryPolicy(RetryPolicy.fixed(0))
                .build()

        server.dispatcher =
            object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    if (request.method == "HEAD") return headResponse(bytes)
                    val current = concurrent.incrementAndGet()
                    maxConcurrent.updateAndGet { max -> maxOf(max, current) }
                    Thread.sleep(50)
                    concurrent.decrementAndGet()
                    return chunkResponse(bytes, request)
                }
            }

        limitedDownloader.download(serverUrl(), outputPath)

        assertTrue(maxConcurrent.get() <= 2, "Max concurrent was ${maxConcurrent.get()}, expected <= 2")
        assertContentEquals(bytes, Files.readAllBytes(outputPath))
    }

    @Test
    fun `adaptive chunking`() {
        val bytes = Random.nextBytes(100)

        val adaptiveDownloader =
            FileDownloader.builder()
                .parallelism(4)
                .chunkSize(20)
                .retryPolicy(RetryPolicy.fixed(2, 0L))
                .adaptiveChunking(true)
                .build()

        server.dispatcher = rangeDispatcher(bytes)
        val report = adaptiveDownloader.download(serverUrl(), outputPath)

        assertContentEquals(bytes, Files.readAllBytes(outputPath))
        assertFalse(report.fallback)
    }

    private fun serverUrl() = server.url("/file").toString()

    private fun rangeDispatcher(data: ByteArray): Dispatcher =
        object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                if (request.method == "HEAD") return headResponse(data)
                return chunkResponse(data, request)
            }
        }

    private fun headResponse(data: ByteArray): MockResponse =
        MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Length", data.size.toString())
            .setHeader("Accept-Ranges", "bytes")

    private fun chunkResponse(
        data: ByteArray,
        request: RecordedRequest,
    ): MockResponse {
        val range = request.getHeader("Range") ?: return MockResponse().setResponseCode(400)
        val (start, end) = range.removePrefix("bytes=").split("-").map { it.toLong() }
        val actualEnd = minOf(end, data.size.toLong() - 1)
        val chunk = data.copyOfRange(start.toInt(), (actualEnd + 1).toInt())
        return MockResponse()
            .setResponseCode(206)
            .setBody(Buffer().write(chunk))
            .setHeader("Content-Range", "bytes $start-$actualEnd/${data.size}")
    }

    private fun sha256(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256")
            .digest(bytes)
            .joinToString("") { "%02x".format(it) }
}
