package downloader

data class RetryPolicy(
    val maxAttempts: Int,
    private val strategy: (Int) -> Long
) {
    fun delayMs(attempt: Int) = strategy(attempt)

    companion object {
        fun fixed(attempts: Int, delayMs: Long = 1000L) =
            RetryPolicy(attempts) { delayMs }

        fun exponential(attempts: Int, baseMs: Long = 500L) =
            RetryPolicy(attempts) { attempt -> baseMs * (1L shl attempt) }
    }
}