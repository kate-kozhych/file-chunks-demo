package downloader.internal.strategy

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ChunkStrategyTest {
    @Test
    fun `exact multiple`() {
        val ranges = ChunkStrategy(256).split(1024)

        assertEquals(4, ranges.size)
        assertEquals(0L..255L, ranges[0])
        assertEquals(256L..511L, ranges[1])
        assertEquals(512L..767L, ranges[2])
        assertEquals(768L..1023L, ranges[3])
    }

    @Test
    fun `last chunk smaller`() {
        val ranges = ChunkStrategy(10).split(25)

        assertEquals(3, ranges.size)
        assertEquals(0L..9L, ranges[0])
        assertEquals(10L..19L, ranges[1])
        assertEquals(20L..24L, ranges[2])
    }

    @Test
    fun `file smaller than chunk`() {
        val ranges = ChunkStrategy(512 * 1024).split(73)

        assertEquals(1, ranges.size)
        assertEquals(0L..72L, ranges[0])
    }

    @Test
    fun `single byte file`() {
        val ranges = ChunkStrategy(1024).split(1)

        assertEquals(1, ranges.size)
        assertEquals(0L..0L, ranges[0])
    }

    @Test
    fun `no gaps between ranges`() {
        val totalBytes = 50L
        val ranges = ChunkStrategy(7).split(totalBytes)

        ranges.zipWithNext { a, b ->
            assertEquals(a.last + 1, b.first)
        }
        assertEquals(0L, ranges.first().first)
        assertEquals(totalBytes - 1, ranges.last().last)
    }

    @Test
    fun `ranges do not overlap`() {
        val ranges = ChunkStrategy(10).split(100)

        ranges.zipWithNext { a, b ->
            assertTrue(a.last < b.first)
        }
    }
}
