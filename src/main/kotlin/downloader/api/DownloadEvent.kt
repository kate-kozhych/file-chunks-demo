package downloader.api

sealed class DownloadEvent {
    data class Started(
        val totalBytes: Long,
        val chunks: Int,
    ) : DownloadEvent()

    data class ChunkCompleted(
        val index: Int,
        val bytesDownloaded: Long,
        val totalBytes: Long,
        val percent: Int,
    ) : DownloadEvent()

    data class ChunkRetrying(
        val index: Int,
        val attempt: Int,
        val reason: String,
    ) : DownloadEvent()

    data class Finished(val report: DownloadReport) : DownloadEvent()
}
