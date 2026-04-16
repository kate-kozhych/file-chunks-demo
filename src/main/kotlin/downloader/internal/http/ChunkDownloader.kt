package downloader.internal.http

import downloader.api.RetryPolicy
import downloader.exception.ChunkDownloadException
import downloader.exception.IncompleteChunkException
import downloader.exception.UnexpectedResponseException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request

internal class ChunkDownloader(private val client: OkHttpClient) {
    data class ChunkResult(val index: Int, val bytes: ByteArray, val durationMs: Long)

    suspend fun download(
        url: String,
        range: LongRange,
        index: Int,
        retryPolicy: RetryPolicy,
    ): ChunkResult =
        withContext(Dispatchers.IO) {
            var lastError: Exception? = null

            repeat(retryPolicy.maxAttempts + 1) { attempt ->
                try {
                    return@withContext downloadOnce(url, range, index)
                } catch (e: Exception) {
                    lastError = e
                    delay(retryPolicy.delayMs(attempt))
                }
            }
            throw ChunkDownloadException("Chunk $index failed after ${retryPolicy.maxAttempts} retries", lastError)
        }

    private fun downloadOnce(
        url: String,
        range: LongRange,
        index: Int,
    ): ChunkResult {
        val start = System.currentTimeMillis()
        val request =
            Request.Builder()
                .url(url)
                .header("Range", "bytes=${range.first}-${range.last}")
                .build()

        return client.newCall(request).execute().use { response ->
            if (response.code != 206) throw UnexpectedResponseException(response.code)
            val bytes = response.body?.bytes() ?: throw UnexpectedResponseException(-1)
            val expected = range.last - range.first + 1
            if (bytes.size.toLong() != expected) throw IncompleteChunkException(expected, bytes.size.toLong())
            ChunkResult(index, bytes, System.currentTimeMillis() - start)
        }
    }
}
