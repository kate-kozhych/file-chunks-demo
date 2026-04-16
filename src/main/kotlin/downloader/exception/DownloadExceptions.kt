package downloader.exception

class ChunkDownloadException(message: String, cause: Throwable?) : Exception(message, cause)

class UnexpectedResponseException(code: Int) : Exception("Unexpected HTTP $code")

class IncompleteChunkException(expected: Long, actual: Long) : Exception("Expected $expected bytes, got $actual")
