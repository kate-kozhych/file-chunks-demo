package downloader.internal.io

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

class FileAssemblerTest {
    @TempDir
    lateinit var tempDir: Path

    @Test
    fun `sequential write`() {
        val path = tempDir.resolve("out.bin")
        val assembler = FileAssembler(path)

        assembler.write(0L, "Hello, ".toByteArray())
        assembler.write(7L, "World!".toByteArray())

        assertContentEquals("Hello, World!".toByteArray(), path.toFile().readBytes())
    }

    @Test
    fun `out of order write`() {
        val path = tempDir.resolve("out.bin")
        val assembler = FileAssembler(path)

        assembler.write(5L, "World".toByteArray())
        assembler.write(0L, "Hello".toByteArray())

        assertContentEquals("HelloWorld".toByteArray(), path.toFile().readBytes())
    }

    @Test
    fun `binary data preserved`() {
        val path = tempDir.resolve("out.bin")
        val assembler = FileAssembler(path)
        val data = ByteArray(256) { it.toByte() }

        assembler.write(0L, data)

        assertContentEquals(data, path.toFile().readBytes())
    }

    @Test
    fun `single byte write`() {
        val path = tempDir.resolve("out.bin")
        val assembler = FileAssembler(path)

        assembler.write(0L, byteArrayOf(42))

        assertEquals(1, path.toFile().length())
        assertEquals(42, path.toFile().readBytes()[0])
    }

    @Test
    fun `multiple chunks same file`() {
        val path = tempDir.resolve("out.bin")
        val assembler = FileAssembler(path)
        val chunk1 = ByteArray(100) { 1 }
        val chunk2 = ByteArray(100) { 2 }
        val chunk3 = ByteArray(100) { 3 }

        assembler.write(200L, chunk3)
        assembler.write(0L, chunk1)
        assembler.write(100L, chunk2)

        val result = path.toFile().readBytes()
        assertEquals(300, result.size)
        assertContentEquals(chunk1, result.copyOfRange(0, 100))
        assertContentEquals(chunk2, result.copyOfRange(100, 200))
        assertContentEquals(chunk3, result.copyOfRange(200, 300))
    }
}
