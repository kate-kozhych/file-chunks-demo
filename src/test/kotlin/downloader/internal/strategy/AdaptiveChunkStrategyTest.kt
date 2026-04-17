package downloader.internal.strategy

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class AdaptiveChunkStrategyTest {
    private val strategy =
        AdaptiveChunkStrategy(
            initialChunkSize = 512 * 1024,
            targetChunkDurationMs = 2000,
            minChunkSize = 64 * 1024,
            maxChunkSize = 8 * 1024 * 1024,
        )

    @Test
    fun `two chunks for large file`() {
        val ranges = strategy.probeRanges(10 * 1024 * 1024)

        assertEquals(2, ranges.size)
        assertEquals(0L, ranges[0].first)
        assertEquals(ranges[0].last + 1, ranges[1].first)
    }

    @Test
    fun `one chunk for small file`() {
        val ranges = strategy.probeRanges(300 * 1024)

        assertEquals(1, ranges.size)
    }

    @Test
    fun `adapted size capped at max`() {
        val adapted =
            strategy.computeAdaptedSize(
                probeDurationsMs = listOf(100L, 100L),
                probeSizeBytes = 512 * 1024,
            )

        assertEquals(8 * 1024 * 1024, adapted)
    }

    @Test
    fun `adapted size clamped at min`() {
        val adapted =
            strategy.computeAdaptedSize(
                probeDurationsMs = listOf(100_000L, 100_000L),
                probeSizeBytes = 512 * 1024,
            )

        assertEquals(64 * 1024, adapted)
    }

    @Test
    fun `empty probes`() {
        val adapted =
            strategy.computeAdaptedSize(
                probeDurationsMs = emptyList(),
                probeSizeBytes = 512 * 1024,
            )

        assertEquals(512 * 1024, adapted)
    }

    @Test
    fun `covers rest of file`() {
        val ranges =
            strategy.remainingRanges(
                totalBytes = 1000,
                startOffset = 400,
                adaptedChunkSize = 200,
            )

        assertEquals(3, ranges.size)
        assertEquals(400L..599L, ranges[0])
        assertEquals(600L..799L, ranges[1])
        assertEquals(800L..999L, ranges[2])
    }

    @Test
    fun `remaining ranges`() {
        val ranges =
            strategy.remainingRanges(
                totalBytes = 1000,
                startOffset = 1000,
                adaptedChunkSize = 200,
            )

        assertTrue(ranges.isEmpty())
    }

    @Test
    fun `no gaps`() {
        val ranges =
            strategy.remainingRanges(
                totalBytes = 500,
                startOffset = 100,
                adaptedChunkSize = 70,
            )

        ranges.zipWithNext { a, b ->
            assertEquals(a.last + 1, b.first)
        }
        assertEquals(100L, ranges.first().first)
        assertEquals(499L, ranges.last().last)
    }
}
