package downloader.internal.strategy

internal class ChunkStrategy(private val chunkSize: Long) {
    fun split(totalBytes: Long): List<LongRange> {
        val ranges = mutableListOf<LongRange>()
        var start = 0L
        while (start < totalBytes) {
            val end = minOf(start + chunkSize - 1, totalBytes - 1)
            ranges.add(start..end)
            start = end + 1
        }
        return ranges
    }
}
