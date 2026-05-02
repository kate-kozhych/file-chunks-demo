# My Architecture and Design Decisions

## Main idea

The task description revolves around three interconnected themes:
**SDK design**, **data transmission**, and **observability/telemetry**.

This downloader is built specifically as a library, not a script. That is why my main priority was to reflects how real SDKs are designed for external consumers. I implemented it with the builder API, the sealed event hierarchy and the structured report. Progress via Kotlin Flow maps directly to real-time observability, so that any downstream system can subscribe, aggregate, or forward events without coupling to the downloader internals. `DownloadReport` is a structured object with throughput, retries, duration, fallback flag. With this design a data ingestion team could easily capture and analyze in future.


## Structure

```
downloader/
├── api/                     ← public surface
│   ├── FileDownloader.kt    ← entry point, builder
│   ├── DownloadEvent.kt     ← sealed event hierarchy
│   ├── DownloadReport.kt    ← structured result / telemetry
│   └── RetryPolicy.kt       ← retry strategy, configurable
├── internal/
│   ├── http/
│   │   ├── MetaFetcher.kt       ← HEAD request + fallback probe
│   │   ├── ChunkDownloader.kt   ← single Range GET
│   │   └── FileMeta.kt          ← parsed server capabilities
│   ├── io/
│   │   └── FileAssembler.kt     ← RandomAccessFile writes by offset
│   └── strategy/
│       ├── ChunkStrategy.kt         ← static range splitting
│       └── AdaptiveChunkStrategy.kt ← probe-and-adjust sizing
└── Main.kt
```


## My key decisions

### 1. Builder pattern for public API

```kotlin
FileDownloader.builder()
    .parallelism(4)
    .chunkSize(512 * 1024)
    .retryPolicy(RetryPolicy.exponential(3))
    .adaptiveChunking(true)
    .build()
```

All fields have safe defaults. The builder separates construction from execution and makes optional configuration explicit.


### 2. channelFlow for progress, not callbacks

`downloadWithProgress` returns `Flow<DownloadEvent>`. The caller decides whether to collect, transform, or ignore it entirely. `download()` (blocking) is implemented on top of the same flow, it just takes the first `Finished` event.

`channelFlow` is used because chunk downloads run in concurrent coroutines via `async`, and `emit` is not safe to call from multiple coroutines. `channelFlow` allows `send` from any coroutine in the scope.

The sealed class `DownloadEvent` has four variants:
- `Started` - total size + chunk count
- `ChunkCompleted` - index, bytes so far, percent
- `ChunkRetrying` - index, attempt number, reason
- `Finished` - full `DownloadReport`

The consumer gets structured data, this makes it composable: filter, map, forward to a metrics system, render in UI.

### 3. Retry ownership is in FileDownloader, not ChunkDownloader

`ChunkDownloader.download()` makes exactly one HTTP request and either returns a result or throws. It has no knowledge of retry policy.

All retry logic lives in `downloadChunkWithRetry()` - a shared helper in `FileDownloader` that handles the while loop, delay, attempt counter, and `ChunkRetrying` event emission. It is used for all remaining chunks in both static and adaptive mode.

Probe chunks in adaptive mode are intentionally downloaded without retry. Retrying a probe would corrupt the timing data used for adaptation: a failed first attempt followed by a fast retry would skew the measured throughput, and the adapted chunk size would not reflect actual network conditions.

### 4. FileAssembler holds one open RAF, synchronized writes

```kotlin
internal class FileAssembler(outputPath: Path) {
    private val raf = RandomAccessFile(outputPath.toFile(), "rw")

    @Synchronized
    fun write(offset: Long, bytes: ByteArray) {
        raf.seek(offset)
        raf.write(bytes)
    }

    fun close() = raf.close()
}
```

One `RandomAccessFile` is opened at construction and closed explicitly via `close()`. In `FileDownloader`, this happens in a `finally` block that covers the entire download, including the adaptive probe phase..

`@Synchronized` handles concurrent writes from parallel chunk coroutines. Each write seeks to its own offset, so chunks can arrive and be written in any order without coordination beyond the lock.


### 5. Fallback instead of failure when Range is unsupported

If `MetaFetcher` detects that the server does not return `Accept-Ranges: bytes` or omits `Content-Length`, the downloader falls back to a single-stream GET instead of throwing. The `DownloadReport` carries `fallback = true` so the caller can observe this. 


### 6. HEAD 405 probe fallback

Some servers reject HEAD but support Range GET. `MetaFetcher` handles this: if HEAD returns 405, it sends a `GET bytes=0-1023` and checks whether the response is 206. If it is, `Content-Range` is parsed for the total size. This makes the downloader work correctly with a wider set of servers without requiring manual configuration.


### 7. Adaptive chunk sizing: probe-and-adjust

When `adaptiveChunking = true`, the download proceeds in two phases.

**Probe phase.** The first two chunks are downloaded via `downloadProbes()`, a separate path from the main download loop. Probe chunks are written to the file and their bytes count toward total progress, but no `ChunkCompleted` events are emitted and no retry policy is applied. `Started` is sent after probes complete, so the event sequence is always `Started → ChunkCompleted* → Finished` regardless of mode.

**Adaptation.** Probe durations are passed to `AdaptiveChunkStrategy.computeAdaptedSize`:

```
throughput = probeBytes / avgDurationMs   (bytes/ms)
adaptedSize = throughput * targetChunkDurationMs
```

Target duration is 2000 ms. The result is clamped to `[64 KB, 8 MB]`. Remaining ranges are split using the adapted size and downloaded through the normal retry-capable path with indices starting at `probeRanges.size`, so chunk indices in progress events are globally unique across both phases.

`AdaptiveChunkStrategy` is a pure class with no HTTP dependency, making it straightforward to test in isolation.

**Trade-off.** Probe failure is not retried and immediately aborts the download. On unreliable connections, prefer `adaptiveChunking(false)` and rely on the standard retry policy.


### 8. Semaphore for parallelism

Concurrency is bounded by a `Semaphore(parallelism)`. All chunk coroutines are launched immediately with `async`, but each acquires the semaphore before making the HTTP call. This ensures backpressure: if `parallelism = 2`, at most 2 chunks are in-flight at any moment. The `FileDownloaderTest.semaphore respects parallelism limit` test verifies this with concurrent counters and artificial delays.


### 9. Output file deleted on failure

If any chunk exhausts its retries, the exception propagates out of `coroutineScope`, is caught in the outer `try/catch`, and `outputPath.deleteIfExists()` is called before rethrowing. I think that in this situation partial file is worse than no file, the caller can safely retry from scratch.


## Tests

Tests are written against `MockWebServer` (OkHttp's test server).

### FileDownloaderTest — integration

| Test | What it verifies |
|---|---|
| `happy path` | Full download, content matches, `fallback = false` |
| `binary integrity` | SHA-256 of result matches original random bytes |
| `out of order chunks` | Chunks with artificial delays still assemble correctly |
| `range header` | Correct `Range: bytes=X-Y` header per chunk |
| `small file` | File smaller than chunk size → 1 chunk |
| `exact size` | File = 2× chunk size → exactly 2 chunks |
| `retry on failure` | One 500 → retry → success, `retries = 1` in report |
| `retries exhausted` | All 500s → `ChunkDownloadException` |
| `fallback` | No `Accept-Ranges` → single GET, `fallback = true` |
| `unknown Content-Length` | Missing length → fallback |
| `head 405` | HEAD 405 → probe GET → parallel download succeeds |
| `output file deleted` | Failure → output file absent after exception |
| `progress flow` | First event `Started`, last `Finished`, final percent = 100 |
| `semaphore respects parallelism` | Peak concurrent requests ≤ `parallelism` |
| `adaptive chunking` | Adaptive mode completes, content matches |

### Unit tests

**ChunkDownloaderTest** - single chunk in isolation: happy path, correct Range header, 200 instead of 206 throws, incomplete bytes throws. 

**MetaFetcherTest** - header parsing: content-length + accept-ranges + etag parsed, HEAD 405 triggers probe, missing headers handled as null/false.

**FileAssemblerTest** - sequential write, out-of-order write, binary data preserved, single byte, multiple chunks in one file.

**ChunkStrategyTest** - exact multiple, last chunk smaller, file smaller than chunk, single byte, no gaps, no overlaps.

**AdaptiveChunkStrategyTest** - probe ranges count, adapted size capped at max, clamped at min, empty probe list, remaining ranges cover exactly the right offset range, no gaps.

## Known limitations and future improvements

### Worker pool instead of semaphore + async-all

Currently all chunk coroutines are created upfront with `async`, and the semaphore gates their execution. For very large files with thousands of chunks, this still allocates all coroutine jobs at once.

A cleaner scaling pattern for future would be to create a fixed pool of worker coroutines consuming ranges from a `Channel`. This bounds both concurrency and memory regardless of chunk count. The tradeoff is more complex orchestration and harder event ordering.

### Resume semantics

On failure, the partial output file is deleted and the caller must restart from zero. This is the right default for simplicity, but for production downloaders I would checkpoint completed chunks so a restart can skip already-downloaded ranges.

Adding this would change `FileAssembler`'s contract (it would need to accept a pre-existing partial file) and require a new public API for resuming a previous session.

### Contention on FileAssembler writes

`FileAssembler` uses `@Synchronized` on a single `RandomAccessFile`, which means all concurrent chunk coroutines serialize on one JVM-level monitor when writing. At high parallelism with small chunk sizes this becomes a bottleneck: coroutines finish their HTTP requests but queue up waiting for the lock. A better approach in production is `FileChannel` with positional writes (`FileChannel.write(buffer, position)`), which allows truly concurrent writes to different offsets without explicit locking.

### ETag is parsed but unused

`MetaFetcher` extracts the `ETag` header and stores it in `FileMeta`, but `FileDownloader` never reads it. I left it here for a production SDK, where ETag serves two purposes: detecting mid-download file changes (if the server rotates the file between the HEAD request and chunk downloads, chunks from different versions get assembled into a corrupt file), and as a cache key for resume semantics (a resumed download should verify the file has not changed since the previous session). Both use cases require threading ETag through to the download loop and sending it as `If-Match` on each range request.