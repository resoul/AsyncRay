# AsyncRay

A lightweight, strongly-typed reactive programming library for Swift built on **Swift Concurrency** (`AsyncStream`, `Task`, `Sendable`) with zero external dependencies.

Designed for modern Swift development, checked in Swift 6 language mode with complete strict concurrency. The manifest retains Swift 5 language-mode support; the installed Swift 6 compiler selects mode 6.

---

## Key Features

- **Swift 6 strict concurrency.** Library sources compile with complete checking; lock-protected primitives use explicit synchronization, and async iterators remain owned by their producer tasks.
- **Native Swift Concurrency, not a Combine wrapper.** Built directly on `AsyncStream` / `Task` / `Sendable` — no bridging layer, no dependency on Combine (unavailable on Linux, increasingly legacy on Apple platforms).
- **Explicit backpressure where fan-in happens.** `merge`, `combineLatest`, `zip`, `flatMap`, `flatMapLatest` and `then` require an explicit `BufferingPolicy`; the unbounded overloads are deprecated. Hot sources (`Pipe`, `asAsyncRay()`) default to a bounded `.bufferingNewest(64)`. Cold sources (`AsyncRay.from`, `AsyncRay.just`, `AsyncRay { emitter in ... }`) are unbounded, so a custom producer that pushes faster than it is consumed should bound itself.
- **Cancellation without leaks or races.** `TerminationStorage` guarantees cleanup handlers still fire with the correct terminal reason even when cancellation happens mid-`await` (e.g. during a socket handshake) — a case most hand-rolled reactive wrappers get wrong.
- **Race-free nested task management.** `TaskBox` atomically replaces and cancels inner tasks (`debounce` timers, `timeout` watchdogs), and `flatMapLatest` serializes switching and delivery on one actor, so a stale inner stream cannot deliver after a switch.
- **Deterministic, RAII-style lifecycle.** `SubscriptionBag` cancels everything stored in it automatically on `deinit`; no manual bookkeeping for bagged subscriptions.
- **Explicit cold/hot separation.** `AsyncRay<T>` is cold by default; `Pipe`, `CurrentValue`/`CurrentValueDistinct`, and `Once` are distinct, clearly-scoped hot primitives — no ambiguity like Combine's `PassthroughSubject` vs `CurrentValueSubject`.
- **Ergonomic push syntax.** The `<-` operator (`events <- "value"`, `events <- [a, b, c]`) makes hot-source emission read naturally without sacrificing the explicit `.send()` API.
- **Zero external dependencies.** Nothing to audit beyond the Swift standard library, the Concurrency runtime and Foundation (`NSLock`, `UUID`) — important for security-sensitive networking code.
- **Protocol-boundary discipline built in.** A documented invariant keeps raw transport frames out of concurrent fan-in operators, preserving strict ordering where it's required (e.g. decrypt → decode → validate → publish).

---

## Table of Contents

- [Key Features](#key-features)
- [Installation](#installation)
- [Architectural Concepts](#architectural-concepts)
- [Quick Start](#quick-start)
- [Core Types](#core-types)
    - [AsyncRay\<T\> (Cold Stream)](#asyncrayt-cold-stream)
    - [Subscription and SubscriptionBag](#subscription-and-subscriptionbag)
    - [AsyncRayEmitter and Safe Cleanup](#asyncrayemitter-and-safe-cleanup)
- [State and Event Primitives](#state-and-event-primitives)
    - [Pipe\<T\> (Hot Push Source)](#pipet-hot-push-source)
    - [CurrentValue\<T\> and CurrentValueDistinct\<T\> (State with Replay)](#currentvaluet-and-currentvaluedistinctt-state-with-replay)
    - [Once\<T\> (One-shot Promise)](#oncet-one-shot-promise)
- [Creating Streams](#creating-streams)
- [Operator Reference](#operator-reference)
    - [Transformation](#transformation)
    - [Filtering](#filtering)
    - [Timing & Rate Limiting](#timing--rate-limiting)
    - [Combining Multiple Streams](#combining-multiple-streams)
    - [Dispatching & Side Effects](#dispatching--side-effects)
    - [Sharing](#sharing)
- [Error Handling](#error-handling)
- [Native Swift Async & Bridges](#native-swift-async--bridges)
- [Order Guarantees](#order-guarantees)
- [Protocol Boundary](#protocol-boundary)
- [Practical Examples](#practical-examples)
    - [Search-as-you-type](#1-search-as-you-type)
    - [Auth Form Validation](#2-auth-form-validation)
    - [Connection State UI Binding](#3-connection-state-ui-binding)
- [Building and Testing](#building-and-testing)
- [Code Style and API Documentation](#code-style-and-api-documentation)
- [Changelog](#changelog)
- [License](#license)

---

## Installation

### Swift Package Manager (SPM)

Add AsyncRay to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/resoul/AsyncRay.git", from: "1.0.0")
]
```

Then add `AsyncRay` to your target's dependencies:

```swift
.target(
    name: "YourApp",
    dependencies: [
        .product(name: "AsyncRay", package: "AsyncRay")
    ]
)
```

Or add via Xcode: **File** → **Add Package Dependencies...** and enter `https://github.com/resoul/AsyncRay.git`.

### Bazel

Add the library to your `BUILD` file:

```python
swift_library(
    name = "MyTarget",
    srcs = glob(["Sources/**/*.swift"]),
    deps = [
        "@asyncray//:AsyncRay",  # When imported as external repository
        # or "//submodules/AsyncRay:AsyncRay" if used as a submodule
    ],
)
```

---

## Architectural Concepts

1. **Cold by default**: `AsyncRay<T>` is a cold reactive stream. Each subscription (`.sink()`, `.sinkOnMain()`, or accessing `.stream`) invokes the producer closure anew and creates an independent `AsyncStream`.
2. **Complete Sendable Safety**: All types are designed for Swift 6 Complete Concurrency — zero unprotected shared mutable state (synchronization via `NSLock` / `actor`).
3. **Deterministic Lifecycle**: Subscriptions are managed explicitly via `Subscription` and `SubscriptionBag` (RAII pattern: all tasks are cancelled automatically upon container `deinit`).
4. **Leak and Race Prevention on Cancellation**:
    - `TerminationStorage` ensures cleanup handlers receive the terminal reason even if cancellation occurs during asynchronous resource initialization (e.g. while awaiting a socket connection).
    - `TaskBox` guarantees atomic replacement and cancellation of nested background tasks (watchdog in `timeout`, debounce delays).
    - `flatMapLatest` switches inner streams, validates their generation and yields on a single actor, so an old inner stream cannot deliver after the switch.
5. **Explicit Backpressure / Buffering Policy**: Multi-source operators (`merge`, `combineLatest`, `zip`, `flatMap`, `flatMapLatest`, `then`) require an explicit `BufferingPolicy` to protect memory from unbounded growth under slow downstream consumers. Hot sources (`Pipe`, `asAsyncRay()`) default to `.bufferingNewest(64)`; unary operators inherit the upstream policy.
6. **Cost model**: Every operator in a chain runs its own `Task` and `AsyncStream` buffer. This keeps operators isolated and cancellable, but a long chain means one task hop per stage — keep hot paths short.

---

## Quick Start

```swift
import AsyncRay

// 1. Create a stream pipeline
let queries = Pipe<String>()

let searchAsyncRay = queries.asyncRay
    .debounce(.milliseconds(300))
    .flatMapLatest(bufferingPolicy: .bufferingNewest(1)) { query in
        api.search(query) // returns AsyncRay<[SearchResult]>
    }

// 2. Subscribe on MainActor
let bag = SubscriptionBag()

searchAsyncRay
    .sinkOnMain { results in
        tableView.reloadData(results)
    }
    .store(in: bag)

// 3. Push input
queries <- "apple"
```

---

## Core Types

### AsyncRay\<T\> (Cold Stream)

`AsyncRay<T: Sendable>` is the fundamental struct of the library, wrapping an `() -> AsyncStream<T>` factory.

- **Callback-based subscription:**
  ```swift
  let sub = asyncRay.sink { value in
      print("Received: \(value)")
  }
  
  // With completion handler:
  let subWithCompletion = asyncRay.sink(
      next: { value in print(value) },
      completed: { print("Stream completed") }
  )
  ```
- **UI Subscription:**
  ```swift
  // Dispatched directly to @MainActor without redundant intermediate streams
  asyncRay.sinkOnMain { [weak self] value in
      self?.label.stringValue = value
  }.store(in: bag)
  ```

### Subscription and SubscriptionBag

- `Subscription`: Thread-safe handle for an active subscription. Calling `sub.cancel()` immediately stops delivery and cancels associated background tasks.
- `SubscriptionBag`: Subscription container. Automatically cancels all stored subscriptions on `deinit` or `bag.cancelAll()`.

> **Note:** A `Subscription` is not cancelled when the handle is released — a discarded subscription keeps running until its stream completes. The compiler warns when a `sink` result is unused; `store(in:)` a bag or keep the handle and call `cancel()`. For a finite stream you deliberately let run to completion, write `_ = stream.sink { ... }`.

```swift
final class ChatViewController: NSViewController {
    private let bag = SubscriptionBag()
    
    override func viewDidLoad() {
        super.viewDidLoad()
        
        chatService.messages.asyncRay
            .sinkOnMain { [weak self] message in
                self?.appendMessage(message)
            }
            .store(in: bag)
    }
    // When ChatViewController deinits, all subscriptions are automatically cancelled
}
```

### AsyncRayEmitter and Safe Cleanup

Use `AsyncRayEmitter<T>` to construct custom `AsyncRay` producers:

```swift
let networkStream = AsyncRay<Data> { emitter in
    let socket = openSocket()
    
    // Register resource cleanup handlers
    emitter.onCancellation {
        socket.close() // called only when subscription is cancelled
    }
    
    emitter.onFinishOrCancel {
        // called on both normal finish and cancellation
    }
    
    socket.onData { data in
        emitter.send(data)
    }
    
    socket.onEnd {
        emitter.finish()
    }
}
```

> **Note:** `AsyncRayEmitter` uses `TerminationStorage`. If you register a cleanup handler after an `await` point (for instance, after awaiting a connection handshake) and the subscription was cancelled in the meantime, the cleanup callback is invoked immediately with the recorded termination reason.

---

## State and Event Primitives

### Pipe\<T\> (Hot Push Source)

A hot multicast broadcast source. Subscribers only receive values emitted **after** they subscribe.

```swift
let events = Pipe<String>(bufferingPolicy: .bufferingNewest(64))

// Subscribe
events.asyncRay.sink { print("Event: \($0)") }.store(in: bag)

// Send via method:
events.send("Message 1")

// Send via operator <- :
events <- "Message 2"
events <- ["Batch 1", "Batch 2"]

// Buffer overflow telemetry:
let results = events.sendObservingOverflow("Critical Event")
if results.contains(where: { if case .dropped = $0 { return true }; return false }) {
    // Handle buffer overflow caused by slow subscriber
}

// Complete the pipe
events.finish()
```

### CurrentValue\<T\> and CurrentValueDistinct\<T\> (State with Replay)

State holder actor.
- New subscribers **immediately receive the current value** (replay = 1), followed by all subsequent state updates.
- Default buffering policy: `.bufferingNewest(1)` (for a slow consumer, only the latest state matters).

```swift
enum ConnectionState: Sendable {
    case disconnected, connecting, connected
}

let state = CurrentValue<ConnectionState>(.disconnected)

// Read current value:
let current = await state.value

// Update state:
await state.set(.connecting)
await state.modify { _ in .connected }

// UI subscription:
state.asyncRay
    .sinkOnMain { status in
        statusLabel.stringValue = "\(status)"
    }
    .store(in: bag)
```

`CurrentValueDistinct<T: Equatable>` behaves identically, but automatically deduplicates consecutive identical values (`set(x)` does not notify if the current value is already `x`).

### Once\<T\> (One-shot Promise)

An actor holding a single asynchronous result.

```swift
let authReady = Once<Bool>()

// In an async initialization task:
await authReady.resolve(true)

// Await directly (throws CancellationError if the waiting task is cancelled):
let isReady = try await authReady.value

// Or reactive subscription (emits 1 value and completes):
authReady.asyncRay.sink { isReady in ... }.store(in: bag)
```

---

## Creating Streams

| Method | Description |
|---|---|
| `AsyncRay.just(value)` | Emits a single `value` and completes immediately. |
| `AsyncRay.empty()` | Completes immediately without emitting any values. |
| `AsyncRay.never()` | A stream that never emits values and never completes. |
| `AsyncRay.from(sequence)` | Emits all elements from a collection/sequence in order, then completes. |
| `AsyncRay.timer(duration)` | Waits for the specified `Duration` and completes without emitting values. |
| `AsyncRay.interval(period)` | Emits `0, 1, 2, ...` every `period` on fixed deadlines and never completes. Keeps only the newest tick for a slow consumer. |
| `AsyncRay { emitter in ... }` | Custom generator closure with full control over emission and lifecycle callbacks. |

---

## Operator Reference

### Transformation

- `map { ... }` — Synchronously transforms upstream values.
- `asyncMap { await ... }` — Asynchronously transforms values with `await` and `Task.isCancelled` checks.
- `compactMap { ... }` — Transforms values and unwraps/filters out `nil` results.
- `scan(initial) { acc, next in ... }` — Emits running intermediate accumulation over elements for each upstream value.
- `reduce(initial) { acc, next in ... }` — Emits single final accumulated result upon normal stream completion (emits `initial` for empty streams).
- `chunks(ofCount:)` — Groups values into arrays of `count` elements; a shorter remainder is emitted when upstream completes.
- `flatMap(maxConcurrent:bufferingPolicy:) { ... }` — Transforms each value into an inner `AsyncRay` and merges results concurrently up to `maxConcurrent` active tasks.
- `flatMapLatest(bufferingPolicy:) { ... }` — Transforms each value into an inner `AsyncRay`, cancelling the previous inner stream when a new outer value arrives. **Completion semantics:** when the outer stream completes, the latest inner stream keeps running and the output completes after it (like Combine's `switchToLatest`), so `AsyncRay.just(query).flatMapLatest(bufferingPolicy: .bufferingNewest(1)) { api.search($0) }` delivers the search result.
- `then(nextAsyncRay, bufferingPolicy:)` — Subscribes to `nextAsyncRay` strictly after the current stream completes.
- `prepend(values...)` — Emits the given values first, then the values of the stream.

### Filtering

- `filter { predicate }` — Forwards only values matching the predicate.
- `take(count)` — Emits the first `count` values and completes.
- `skip(count)` — Drops the first `count` values.
- `prefix(while:)` — Emits values as long as the predicate holds `true`, then completes.
- `drop(while:)` — Drops values as long as the predicate holds `true`, then emits all remaining values.
- `skipRepeats()` — Drops consecutive duplicate values (requires `T: Equatable`).
- `skipRepeats(by:)` — Drops a value when a custom equivalence matches it to the previously forwarded value (e.g. `{ $0.id == $1.id }`).

### Timing & Rate Limiting

- `delay(duration)` — Shifts the timeline by `duration`: each value is delivered `duration` after it arrived, preserving order and spacing. Values arriving together are delivered together (the delay does not accumulate).
- `debounce(duration)` — Waits for a quiet period of `duration` before delivering the **latest** value (ideal for text search fields).
- `throttle(duration)` — Limits emission rate to at most once per `duration` (leading throttle — delivers the **first** value in each window).
- `throttle(duration, latest: true)` — Delivers the first value immediately, then at the end of each window the **latest** value received during it (trailing throttle, useful for scroll or slider updates). A pending value is delivered before completion.
- `timeout(duration)` — Completes the stream if no values are received within `duration`.

Each timing operator, `AsyncRay.timer`, and `AsyncRay.interval` also accept a `clock:` argument (any `Clock` whose `Duration` is `Swift.Duration`). The default is `ContinuousClock`; pass a manual test clock to drive time deterministically in tests:

```swift
let results = query.asyncRay.debounce(.milliseconds(300), clock: testClock)
```

### Combining Multiple Streams

- `merge([asyncRay1, asyncRay2], bufferingPolicy:)` — Merges values from multiple streams of the same type as they arrive.
- `combineLatest(fa, fb, bufferingPolicy:)` — Combines latest values from 2, 3, or 4 streams into a tuple whenever any input changes (starts emitting once each source has emitted at least once). **Completion semantics:** Output completes only when *all* input streams complete; if one input stream finishes while others continue, new emissions continue pairing with the finished stream's last value. Tuples are emitted in the order state changes, so the last emitted tuple always reflects the latest value of every input.
- `zip(fa, fb, bufferingPolicy:)` — Pairs values from two streams 1-to-1 concurrently. If either stream finishes or the subscription is cancelled, pending reads are cancelled and the stream completes.
- `withLatestFrom(other)` — Pairs each value with the latest value of `other`; emits only when the source emits, drops source values that arrive before `other` has emitted, and completes with the source.

### Dispatching & Side Effects

- `onMain()` — Dispatches downstream value delivery to `MainActor`.
- `sinkOnMain { ... }` — Directly subscribes on `MainActor` without intermediate stream allocations (recommended for UI).
- `onBackground(priority:)` — Dispatches delivery inside a detached background task with the specified priority.
- `handleEvents { ... }` — Executes a side effect for each value without mutating the stream (logging/metrics).
- `onCompletion { ... }` — Executes a side effect upon normal stream completion (does not fire on cancellation).

### Sharing

- `share(bufferingPolicy:)` — Shares one upstream subscription among all subscribers. Upstream starts with the first subscriber and is cancelled when the last one leaves; after that, or after upstream completes, the next subscriber starts a fresh upstream subscription. No values are replayed, and each subscriber has its own buffer (default `.bufferingNewest(64)`).

```swift
let profile = api.fetchProfile().share()
profile.sinkOnMain { header.update($0) }.store(in: bag)
profile.sinkOnMain { avatar.update($0) }.store(in: bag)  // one request, two subscribers
```

---

## Error Handling

`AsyncRay<T>` never fails. For streams that can, use `AsyncThrowingRay<T>`, the failable counterpart built on `AsyncThrowingStream`. It is also cold, and it ends in exactly one way: normal completion, failure with an error, or cancellation. Cancellation reports neither completion nor failure.

```swift
// One async operation per subscription
let user = AsyncThrowingRay.from { try await api.fetchUser(id) }

// A custom producer that can fail
let frames = AsyncThrowingRay<Data> { emitter in
    let socket = openSocket()
    emitter.onCancellation { socket.close() }
    socket.onFrame { emitter.send($0) }
    socket.onError { emitter.fail($0) }
}

// A failing step inside a non-failing pipeline
let messages = pipe.asyncRay.tryMap { try JSONDecoder().decode(Message.self, from: $0) }

// Handle the failure in the subscriber...
user.sinkOnMain(
    next: { profileView.show($0) },
    failure: { errorBanner.show($0) }
).store(in: bag)

// ...or recover and continue as a non-failing AsyncRay
user
    .retry(2, delay: .seconds(1))
    .catch { _ in AsyncRay.just(.guest) }
    .sinkOnMain { profileView.show($0) }
    .store(in: bag)
```

| API | Description |
| --- | --- |
| `AsyncThrowingRay.from { try await ... }` | Runs one async throwing operation per subscription; emits its result or fails. |
| `AsyncThrowingRay { emitter in ... }` | Custom producer; `emitter.fail(error)` ends the stream with an error. |
| `just`, `from(sequence)`, `fail(error)`, `empty()` | Basic sources. |
| `map`, `tryMap`, `asyncMap`, `compactMap`, `filter` | Transformations; `tryMap` and `asyncMap` may throw, which fails the stream and cancels upstream. |
| `onFailure { error in ... }` | Side effect on failure (logging); forwards the failure. |
| `retry(n)`, `retry(n, delay:)`, `retry(n, delay:clock:)` | Resubscribes after a failure up to `n` times. Values from failed attempts are already delivered. |
| `catch { error in AsyncRay<T> }` | Switches to a fallback stream on failure; returns a non-failing `AsyncRay`. |
| `replaceError(with:)` | Emits one fallback value on failure and completes; returns `AsyncRay`. |
| `asResult()` | Materializes values and the failure as `Result`s in a non-failing `AsyncRay`. |
| `sink(next:failure:completed:)`, `sinkOnMain(next:failure:completed:)` | Subscriptions; exactly one of `failure` or `completed` runs unless cancelled. |
| `stream`, `first()`, `collect()`, `for try await` | Native async access; `first()` and `collect()` rethrow the failure. |
| `asyncRay.asThrowing()`, `asyncRay.tryMap { ... }` | Move from `AsyncRay` into the failable world. |

Inside a `flatMapLatest` (for example, search-as-you-type), recover per request so that one failed request does not end the whole pipeline:

```swift
query.asyncRay
    .debounce(.milliseconds(300))
    .flatMapLatest(bufferingPolicy: .bufferingNewest(1)) { text in
        AsyncThrowingRay.from { try await api.search(text) }.replaceError(with: [])
    }
    .sinkOnMain { resultsView.show($0) }
    .store(in: bag)
```

---

## Native Swift Async & Bridges

AsyncRay integrates seamlessly with standard Swift Concurrency:

```swift
// 1. Iterate using for await
for await value in asyncRay.stream {
    print(value)
}

// 2. Fetch first value
if let first = await asyncRay.first() {
    print("First value: \(first)")
}

// 3. Collect all values of a finite stream into an array
let allValues = await asyncRay.collect()

// 4. Convert native AsyncStream into AsyncRay (lazy, ref-counted shared multicast bridge)
let asyncRayFromStream = nativeAsyncStream.asAsyncRay()

// 5. Convert to throwing stream (for APIs that expect one; AsyncRay itself never throws —
//    see AsyncThrowingRay for streams that can fail)
let throwing = asyncRay.throwingStream
```

---

## Order Guarantees

| Category | Operators | Order Guarantee |
|---|---|---|
| **Sequential** | `map`, `filter`, `compactMap`, `take`, `skip`, `prefix`, `drop`, `then`, `prepend`, `skipRepeats`, `scan`, `chunks` | Result strictly follows upstream order. |
| **Time-filtering** | `debounce`, `throttle` | Relative order of delivered values is preserved (intermediate items may be dropped). |
| **Time-shifting** | `delay` | Upstream order and spacing are preserved; every value is shifted by the same duration. |
| **Concurrent (Fan-in / Multi-source)** | `merge`, `flatMap`, `combineLatest`, `zip`, `withLatestFrom` | Order between sources depends on parallel task completion and runtime scheduling. |
| **Switch-to-latest** | `flatMapLatest` | Previous inner stream is cancelled when a new outer value arrives; no value from it is delivered after the switch. Output completes after both the outer and the latest inner complete. |

---

## Protocol Boundary

> [!IMPORTANT]
> **Network Transport Invariant:**
>
> AsyncRay is strictly intended for use **after protocol decoding and validation** — once raw network frames have been decrypted, validated, and converted into domain events (`IncomingEvent`).
>
> The read loop remains strictly sequential:
> $$\text{read frame} \longrightarrow \text{decrypt / verify} \longrightarrow \text{decode} \longrightarrow \text{validate} \longrightarrow \text{demux} \longrightarrow \text{publish to AsyncRay}$$
>
> Raw frames should not pass through concurrent fan-in operators (`merge`, `flatMap`, `combineLatest`) to preserve frame ordering and transport context.

---

## Practical Examples

### 1. Search-as-you-type

```swift
final class SearchViewModel {
    let queryPipe = Pipe<String>()
    private let bag = SubscriptionBag()
    
    @MainActor var searchResults: [SearchResult] = []
    
    init(api: SearchAPI) {
        queryPipe.asyncRay
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { $0.count >= 2 }
            .debounce(.milliseconds(300))
            .skipRepeats()
            .flatMapLatest(bufferingPolicy: .bufferingNewest(1)) { query in
                api.search(query) // cancels previous in-flight network requests on new input
            }
            .sinkOnMain { [weak self] results in
                self?.searchResults = results
            }
            .store(in: bag)
    }
}
```

### 2. Auth Form Validation

```swift
let email = CurrentValue("")
let password = CurrentValue("")

let isFormValid = combineLatest(
    email.asyncRay,
    password.asyncRay,
    bufferingPolicy: .bufferingNewest(1)
).map { email, pass in
    email.contains("@") && pass.count >= 8
}

isFormValid
    .sinkOnMain { isValid in
        loginButton.isEnabled = isValid
    }
    .store(in: bag)
```

### 3. Connection State UI Binding

```swift
client.connectionState?.sinkOnMain { [weak self] state in
    switch state {
    case .connected:
        self?.statusIndicator.color = .systemGreen
    case .reconnecting(let attempt):
        self?.statusIndicator.color = .systemOrange
    case .disconnected:
        self?.statusIndicator.color = .systemRed
    }
}.store(in: bag)
```

---

## Building and Testing

### Running Tests

```bash
# Swift Package Manager:
swift test

# Bazel:
bazel test //:AsyncRayTests
```

### Swift language-mode verification

```sh
make build-swift6
make test-swift6
make test-swift5
```

The test suite includes a `ManualClock` ([`Tests/ManualClock.swift`](Tests/ManualClock.swift)) whose
time moves only when a test calls `advance(by:)`; timing-operator tests built on it
([`Tests/ClockTests.swift`](Tests/ClockTests.swift)) assert exact deadlines without wall-clock sleeps.

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs `lint-format`, `build-swift6`,
`test-swift6` and `test-swift5` on macOS.

Library compilation treats warnings as errors. Tests that exercise deprecated compatibility
overloads are themselves marked `@available(*, deprecated)`, so their intentional calls do not
produce deprecation warnings.

### Code Style and API Documentation

The repository uses `swift-format`; its settings live in [`.swift-format`](.swift-format).
`make lint-format` runs the strict check. Format a file when you work on it, or use the full
source and test trees when making a deliberate repository-wide formatting pass:

```sh
# Report findings across the source and test trees
make lint-format

# Format one file while working on it
make format-swift SWIFT_PATHS=Sources/Core/AsyncRay.swift
```

When documenting public APIs, explain behavior that affects how callers use them: ownership and
cancellation, actor isolation, buffering and possible value loss, ordering, completion, or errors.
Include only the topics that apply to that API. Avoid file-title comments above imports; put useful
implementation context after imports and API documentation beside the declaration it describes.

---

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for detailed release history and changes.

---

## License

AsyncRay is released under the [MIT License](LICENSE).
