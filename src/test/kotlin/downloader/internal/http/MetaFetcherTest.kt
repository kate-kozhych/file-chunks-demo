package downloader.internal.http

import kotlinx.coroutines.test.runTest
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class MetaFetcherTest {
    private lateinit var server: MockWebServer
    private val fetcher = MetaFetcher(OkHttpClient())

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
    fun `parses headers`() =
        runTest {
            server.enqueue(
                MockResponse()
                    .setResponseCode(200)
                    .setHeader("Content-Length", "1024")
                    .setHeader("Accept-Ranges", "bytes")
                    .setHeader("ETag", "abc123"),
            )

            val meta = fetcher.fetch(serverUrl())

            assertEquals(1024L, meta.contentLength)
            assertTrue(meta.acceptsRanges)
            assertEquals("abc123", meta.etag)
        }

    @Test
    fun `HEAD 405 probe GET`() =
        runTest {
            server.enqueue(MockResponse().setResponseCode(405))
            server.enqueue(
                MockResponse()
                    .setResponseCode(206)
                    .setHeader("Content-Range", "bytes 0-1023/5000"),
            )

            val meta = fetcher.fetch(serverUrl())

            assertEquals(5000L, meta.contentLength)
            assertTrue(meta.acceptsRanges)
        }

    @Test
    fun `missing Accept-Ranges`() =
        runTest {
            server.enqueue(
                MockResponse()
                    .setResponseCode(200)
                    .setHeader("Content-Length", "500"),
            )

            val meta = fetcher.fetch(serverUrl())

            assertFalse(meta.acceptsRanges)
        }

    @Test
    fun `missing Content-Length`() =
        runTest {
            server.enqueue(
                MockResponse()
                    .setResponseCode(200)
                    .setHeader("Accept-Ranges", "bytes"),
            )

            val meta = fetcher.fetch(serverUrl())

            assertTrue(meta.contentLength == null || meta.contentLength == 0L)
        }

    @Test
    fun `range not supported`() =
        runTest {
            server.enqueue(MockResponse().setResponseCode(405))
            server.enqueue(
                MockResponse()
                    .setResponseCode(200)
                    .setHeader("Content-Length", "1000"),
            )

            val meta = fetcher.fetch(serverUrl())

            assertFalse(meta.acceptsRanges)
        }

    private fun serverUrl() = server.url("/").toString()
}
