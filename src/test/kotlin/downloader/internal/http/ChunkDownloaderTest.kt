package downloader.internal.http

import downloader.api.RetryPolicy
import downloader.exception.ChunkDownloadException
import kotlinx.coroutines.test.runTest
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okio.Buffer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ChunkDownloaderTest {
    private lateinit var server: MockWebServer
    private val downloader = ChunkDownloader(OkHttpClient())
    private val noRetry = RetryPolicy.fixed(0)
    private val twoRetries = RetryPolicy.fixed(2, 0L)

    @BeforeEach
    fun setup() {
        server = MockWebServer()
        server.start()
    }

    @AfterEach
    fun teardown() {
        server.shutdown()
    }

    @Test
    fun `happy path`() =
        runTest {
            val data = Random.nextBytes(1024)
            server.enqueue(
                MockResponse()
                    .setResponseCode(206)
                    .setBody(Buffer().write(data))
                    .setHeader("Content-Range", "bytes 0-1023/1024"),
            )

            val result = downloader.download(serverUrl(), 0L..1023L, 0, noRetry)

            assertEquals(1024, result.bytes.size)
            assertTrue(result.bytes.contentEquals(data))
        }

    @Test
    fun `range header sent correctly`() =
        runTest {
            val data = "hello".toByteArray()
            server.enqueue(
                MockResponse()
                    .setResponseCode(206)
                    .setBody(Buffer().write(data))
                    .setHeader("Content-Range", "bytes 10-14/100"),
            )

            downloader.download(serverUrl(), 10L..14L, 0, noRetry)

            val request = server.takeRequest()
            assertEquals("bytes=10-14", request.getHeader("Range"))
        }

    @Test
    fun `unexpected 200 throws`() =
        runTest {
            server.enqueue(
                MockResponse()
                    .setResponseCode(200)
                    .setBody(Buffer().write("full file".toByteArray())),
            )

            assertFailsWith<ChunkDownloadException> {
                downloader.download(serverUrl(), 0L..4L, 0, noRetry)
            }
        }

    @Test
    fun `incomplete chunk retried`() =
        runTest {
            val full = "hello world".toByteArray()
            val partial = "hel".toByteArray()

            server.enqueue(
                MockResponse()
                    .setResponseCode(206)
                    .setBody(Buffer().write(partial))
                    .setHeader("Content-Range", "bytes 0-10/100"),
            )
            server.enqueue(
                MockResponse()
                    .setResponseCode(206)
                    .setBody(Buffer().write(full))
                    .setHeader("Content-Range", "bytes 0-10/100"),
            )

            val result = downloader.download(serverUrl(), 0L..10L, 0, twoRetries)

            assertEquals(11, result.bytes.size)
            assertTrue(result.bytes.contentEquals(full))
        }

    @Test
    fun `retry recovers`() =
        runTest {
            val data = Random.nextBytes(512)
            server.enqueue(MockResponse().setResponseCode(500))
            server.enqueue(
                MockResponse()
                    .setResponseCode(206)
                    .setBody(Buffer().write(data))
                    .setHeader("Content-Range", "bytes 0-511/512"),
            )

            val result = downloader.download(serverUrl(), 0L..511L, 0, twoRetries)

            assertEquals(512, result.bytes.size)
            assertTrue(result.bytes.contentEquals(data))
        }

    @Test
    fun `retries exhausted`() =
        runTest {
            repeat(3) { server.enqueue(MockResponse().setResponseCode(500)) }

            assertFailsWith<ChunkDownloadException> {
                downloader.download(serverUrl(), 0L..1023L, 0, twoRetries)
            }
        }

    private fun serverUrl() = server.url("/").toString()
}
