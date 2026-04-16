package downloader.api

data class DownloadReport(
    val totalBytes: Long,
    val durationMs: Long,
    val chunksCount: Int,
    val retries: Int,
    val fallback: Boolean,
    val throughputMBps: Double = totalBytes / 1024.0 / 1024.0 / (durationMs / 1000.0),
)
