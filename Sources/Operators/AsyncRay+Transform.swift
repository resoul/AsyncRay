// Stream transformation operators

extension AsyncRay {

    // MARK: - map

    /// Synchronously transforms each value.
    ///
    /// ```swift
    /// asyncRay.map { $0 * 2 }.sink { print($0) }
    /// ```
    public func map<U: Sendable>(
        _ transform: @Sendable @escaping (T) -> U
    ) -> AsyncRay<U> {
        chained { continuation, stream in
            let task = Task {
                for await value in stream {
                    continuation.yield(transform(value))
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }

    /// Asynchronously transforms each value.
    ///
    /// ```swift
    /// asyncRay.asyncMap { id in await api.fetchUser(id) }
    /// ```
    public func asyncMap<U: Sendable>(
        _ transform: @Sendable @escaping (T) async -> U
    ) -> AsyncRay<U> {
        chained { continuation, stream in
            let task = Task {
                for await value in stream {
                    guard !Task.isCancelled else { break }
                    continuation.yield(await transform(value))
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }

    // MARK: - compactMap

    /// Transforms each value and unwraps non-nil results.
    ///
    /// ```swift
    /// asyncRay.compactMap { Int($0) }  // String -> Int? -> Int
    /// ```
    public func compactMap<U: Sendable>(
        _ transform: @Sendable @escaping (T) -> U?
    ) -> AsyncRay<U> {
        chained { continuation, stream in
            let task = Task {
                for await value in stream {
                    if let mapped = transform(value) {
                        continuation.yield(mapped)
                    }
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }

    // MARK: - flatMap

    /// Transforms each value into a child `AsyncRay` and merges results concurrently,
    /// bounding concurrency to at most `maxConcurrent` active inner subscriptions.
    ///
    /// ```swift
    /// userIds.asyncRay.flatMap(maxConcurrent: 4, bufferingPolicy: .bufferingNewest(64)) {
    ///     id in api.fetchUser(id)
    /// }
    /// ```
    ///
    /// Contract:
    /// - At most `maxConcurrent` active inner subscriptions at any given time.
    /// - Delivery order between concurrent inner streams is not guaranteed.
    /// - Completion of outer stream waits for all in-flight inner tasks to complete.
    /// - Cancellation of the outer subscription cancels all active inner tasks.
    /// - Reading from upstream pauses while awaiting a free concurrency slot.
    public func flatMap<U: Sendable>(
        maxConcurrent: Int,
        bufferingPolicy: AsyncStream<U>.Continuation.BufferingPolicy,
        _ transform: @Sendable @escaping (T) -> AsyncRay<U>
    ) -> AsyncRay<U> {
        precondition(maxConcurrent > 0, "flatMap(maxConcurrent:) requires maxConcurrent > 0")
        return AsyncRay<U>(bufferingPolicy: bufferingPolicy) { [_make] in
            AsyncStream<U>(bufferingPolicy: bufferingPolicy) { continuation in
                let outerTask = Task {
                    await withTaskGroup(of: Void.self) { group in
                        var active = 0
                        for await value in _make() {
                            if active >= maxConcurrent {
                                await group.next()
                                active -= 1
                            }
                            let inner = transform(value)
                            active += 1
                            group.addTask {
                                for await v in inner._make() {
                                    continuation.yield(v)
                                }
                            }
                        }
                        await group.waitForAll()
                    }
                    guard !Task.isCancelled else { return }
                    continuation.finish()
                }
                continuation.onTermination = { @Sendable _ in outerTask.cancel() }
            }
        }
    }

    /// Deprecated unbounded `flatMap` retained for source compatibility.
    @available(*, deprecated, message: "Use flatMap(maxConcurrent:bufferingPolicy:_:) — an unbounded flatMap can grow inner subscriptions and output buffering without limit.")
    public func flatMap<U: Sendable>(
        _ transform: @Sendable @escaping (T) -> AsyncRay<U>
    ) -> AsyncRay<U> {
        flatMap(maxConcurrent: .max, bufferingPolicy: .unbounded, transform)
    }

    // MARK: - flatMapLatest

    /// Transforms each value into a child `AsyncRay`, cancelling the previous inner stream
    /// when a new outer value arrives. Generation validation and output yield are serialized
    /// with replacement, so an old inner cannot enqueue values after the switch.
    /// Values already buffered before the switch are not retracted.
    ///
    /// **Completion semantics:** When the outer stream completes, the latest inner stream
    /// keeps running and output finishes once it completes (matching Combine's `switchToLatest`
    /// and Rx `switchMap`). Cancelling the subscription cancels the active inner immediately.
    ///
    /// ```swift
    /// searchQuery.asyncRay
    ///     .debounce(.milliseconds(300))
    ///     .flatMapLatest(bufferingPolicy: .bufferingNewest(1)) { query in api.search(query) }
    ///     .sinkOnMain { results in tableView.reload(results) }
    /// ```
    public func flatMapLatest<U: Sendable>(
        bufferingPolicy: AsyncStream<U>.Continuation.BufferingPolicy,
        _ transform: @Sendable @escaping (T) -> AsyncRay<U>
    ) -> AsyncRay<U> {
        AsyncRay<U>(bufferingPolicy: bufferingPolicy) { [_make] in
            AsyncStream<U>(bufferingPolicy: bufferingPolicy) { continuation in
                let latest = _LatestSubscription(continuation: continuation)
                let outerTask = Task {
                    for await value in _make() {
                        guard !Task.isCancelled else { break }
                        await latest.replace(with: transform(value))
                    }
                    if Task.isCancelled {
                        await latest.finish()
                    } else {
                        await latest.outerCompleted()
                    }
                }
                continuation.onTermination = { @Sendable _ in
                    outerTask.cancel()
                    Task { await latest.finish() }
                }
            }
        }
    }

    /// Deprecated unbounded overload retained for source compatibility.
    @available(*, deprecated, message: "Use flatMapLatest(bufferingPolicy:_:) — an unbounded output stream can grow without limit under a slow downstream consumer.")
    public func flatMapLatest<U: Sendable>(
        _ transform: @Sendable @escaping (T) -> AsyncRay<U>
    ) -> AsyncRay<U> {
        flatMapLatest(bufferingPolicy: .unbounded, transform)
    }

    // MARK: - then

    /// Chains another stream to run strictly after the current stream completes, with an explicit output buffering policy.
    ///
    /// ```swift
    /// AsyncRay.just(1).then(AsyncRay.just(2), bufferingPolicy: .bufferingNewest(16)).collect()  // [1, 2]
    /// ```
    public func then(
        _ next: AsyncRay<T>,
        bufferingPolicy: AsyncStream<T>.Continuation.BufferingPolicy
    ) -> AsyncRay<T> {
        AsyncRay(bufferingPolicy: bufferingPolicy) { [_make] in
            AsyncStream<T>(bufferingPolicy: bufferingPolicy) { continuation in
                let task = Task {
                    for await value in _make() { continuation.yield(value) }
                    for await value in next._make() { continuation.yield(value) }
                    guard !Task.isCancelled else { return }
                    continuation.finish()
                }
                continuation.onTermination = { @Sendable _ in task.cancel() }
            }
        }
    }

    /// Deprecated unbounded overload retained for source compatibility.
    @available(*, deprecated, message: "Use then(_:bufferingPolicy:) — an unbounded output stream can grow without limit under a slow downstream consumer.")
    public func then(_ next: AsyncRay<T>) -> AsyncRay<T> {
        then(next, bufferingPolicy: .unbounded)
    }
}
