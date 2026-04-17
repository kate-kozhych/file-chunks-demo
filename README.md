# Parallel File Downloader

Kotlin SDK for parallel chunked file downloads over HTTP with progress streaming and adaptive chunk sizing.

## Stack

- **Kotlin** + **Coroutines / Flow**
- **OkHttp** - HTTP client
- **JUnit 5** + **MockWebServer** - testing


## Run

**Requirements:** JDK 21, Gradle (or use `./gradlew`)

```bash
# Run tests
./gradlew test

# Run main demo (requires local server)
./gradlew run
```

**Start a local file server:**

```bash
docker run --rm -p 8080:80 \
  -v /path/to/your/dir:/usr/local/apache2/htdocs/ \
  httpd:latest
```

Then put any file in that directory and access it at `http://localhost:8080/yourfile`.

The `Main.kt` entry point targets `http://localhost:8080/test.txt` by default change the URL and output path as needed.


## Usage

**Simple download (blocking):**

```kotlin
val downloader = FileDownloader.builder()
    .parallelism(4)
    .chunkSize(512 * 1024)
    .retryPolicy(RetryPolicy.exponential(3))
    .build()

val report = downloader.download(url, outputPath)
println("${report.throughputMBps} MB/s")
```

**With progress (Flow):**

```kotlin
downloader.downloadWithProgress(url, outputPath).collect { event ->
    when (event) {
        is DownloadEvent.Started       -> println("${event.totalBytes} bytes, ${event.chunks} chunks")
        is DownloadEvent.ChunkCompleted -> println("${event.percent}%")
        is DownloadEvent.ChunkRetrying  -> println("Chunk ${event.index} retry #${event.attempt}")
        is DownloadEvent.Finished       -> println(event.report)
    }
}
```

**Adaptive chunk sizing:**

```kotlin
FileDownloader.builder()
    .adaptiveChunking(true)   // probes first 2 chunks, adjusts size automatically
    .build()
```


## Builder Options

| Method | Default | Description |
|---|---|---|
| `parallelism(n)` | 4 | Max concurrent chunk downloads |
| `chunkSize(bytes)` | 512 KB | Initial chunk size |
| `retryPolicy(policy)` | fixed(3) | `RetryPolicy.fixed(n)` or `RetryPolicy.exponential(n)` |
| `adaptiveChunking(bool)` | false | Auto-tune chunk size based on measured throughput |
| `httpClient(client)` | default | Custom `OkHttpClient` |


## DownloadReport

Returned after every download (via `Finished` event or `download()` return value):

```
totalBytes    - file size
durationMs    - wall clock time
chunksCount   - number of chunks used
retries       - total retry count across all chunks
fallback      - true if server did not support Range requests
throughputMBps - computed automatically
```


## Fallback Behaviour

If the server does not support `Accept-Ranges` or omits `Content-Length`, the downloader automatically falls back to a single-stream GET. The report will have `fallback = true`. No exception is thrown.