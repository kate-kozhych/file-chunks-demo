package downloader.internal.strategy

internal class AdaptiveChunkStrategy(
    private val initialChunkSize: Long = 512 * 1024,
    private val targetChunkDurationMs: Long = 2000,
    private val minChunkSize: Long = 64 * 1024,
    private val maxChunkSize: Long = 8 * 1024 * 1024,
) {
    fun probeRanges(totalBytes: Long): List<LongRange> {
        val ranges = mutableListOf<LongRange>()
        var start = 0L

        repeat(2) {
            if (start >= totalBytes) return@repeat
            val end = minOf(start + initialChunkSize - 1, totalBytes - 1)
            ranges.add(start..end)
            start = end + 1
        }

        return ranges
    }

    fun computeAdaptedSize(
        probeDurationsMs: List<Long>,
        probeSizeBytes: Long,
    ): Long {
        if (probeDurationsMs.isEmpty()) return initialChunkSize

        val avgDurationMs = probeDurationsMs.average().coerceAtLeast(1.0)
        val throughputBytesPerMs = probeSizeBytes.toDouble() / avgDurationMs
        val adapted = (throughputBytesPerMs * targetChunkDurationMs).toLong()

        return adapted.coerceIn(minChunkSize, maxChunkSize)
    }

    fun remainingRanges(
        totalBytes: Long,
        startOffset: Long,
        adaptedChunkSize: Long,
    ): List<LongRange> {
        if (startOffset >= totalBytes) return emptyList()

        val ranges = mutableListOf<LongRange>()
        var start = startOffset

        while (start < totalBytes) {
            val end = minOf(start + adaptedChunkSize - 1, totalBytes - 1)
            ranges.add(start..end)
            start = end + 1
        }

        return ranges
    }
}
