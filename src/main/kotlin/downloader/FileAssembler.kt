package downloader

import java.io.RandomAccessFile
import java.nio.file.Path

internal class FileAssembler(private val outputPath: Path) {
    fun write(
        offset: Long,
        bytes: ByteArray,
    ) {
        RandomAccessFile(outputPath.toFile(), "rw").use { file ->
            file.seek(offset)
            file.write(bytes)
        }
    }
}
