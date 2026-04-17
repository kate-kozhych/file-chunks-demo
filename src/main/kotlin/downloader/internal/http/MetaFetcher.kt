package downloader.internal.http

import okhttp3.OkHttpClient
import okhttp3.Request

internal class MetaFetcher(private val client: OkHttpClient) {
    fun fetch(url: String): FileMeta {
        val headResponse =
            client.newCall(
                Request.Builder().url(url).head().build(),
            ).execute()

        if (headResponse.code == 405) return probeViaGet(url)
        return headResponse.use { parseFileMeta(it) }
    }

    private fun probeViaGet(url: String): FileMeta {
        return client.newCall(
            Request.Builder().url(url).header("Range", "bytes=0-1023").build(),
        ).execute().use { response ->
            val rangeSupported = response.code == 206
            val totalLength =
                response.header("Content-Range")
                    ?.substringAfter("/")?.toLongOrNull()
            FileMeta(totalLength, rangeSupported, response.header("ETag"))
        }
    }

    private fun parseFileMeta(response: okhttp3.Response): FileMeta {
        val contentLength = response.header("Content-Length")?.toLongOrNull()
        val acceptsRanges = response.header("Accept-Ranges")?.lowercase() == "bytes"
        return FileMeta(contentLength, acceptsRanges, response.header("ETag"))
    }
}
