package downloader

data class FileMeta(
    val contentLength: Long?,
    val acceptsRanges: Boolean,
    val etag: String?,
)
