package downloader.internal.io

import java.io.RandomAccessFile
import java.nio.file.Path

internal class FileAssembler(outputPath: Path) {
    private val raf = RandomAccessFile(outputPath.toFile(), "rw")

    @Synchronized
    fun write(
        offset: Long,
        bytes: ByteArray,
    ) {
        raf.seek(offset)
        raf.write(bytes)
    }

    fun close() = raf.close()
}
