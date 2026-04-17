package downloader.internal.http

import downloader.exception.IncompleteChunkException
import downloader.exception.UnexpectedResponseException
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

            val result = downloader.download(serverUrl(), 0L..1023L, 0)

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

            downloader.download(serverUrl(), 10L..14L, 0)

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

            assertFailsWith<UnexpectedResponseException> {
                downloader.download(serverUrl(), 0L..4L, 0)
            }
        }

    @Test
    fun `incomplete chunk throws`() =
        runTest {
            val partial = "hel".toByteArray()
            server.enqueue(
                MockResponse()
                    .setResponseCode(206)
                    .setBody(Buffer().write(partial))
                    .setHeader("Content-Range", "bytes 0-10/100"),
            )

            assertFailsWith<IncompleteChunkException> {
                downloader.download(serverUrl(), 0L..10L, 0)
            }
        }

    private fun serverUrl() = server.url("/").toString()
}