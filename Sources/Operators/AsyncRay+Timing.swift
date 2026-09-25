// Time and rate-limiting operators: debounce, throttle, delay, timeout

extension AsyncRay {

    // MARK: - delay

    /// Shifts the whole timeline by `duration`: each value is delivered `duration` after
    /// it arrived from upstream, preserving order and the spacing between values.
    ///
    /// Values arriving together are delivered together after one `duration`, not one after
    /// another with an accumulating delay. Upstream completion is delivered after the last
    /// delayed value. Cancellation drops values that are still in flight.
    ///
    /// ```swift
    /// asyncRay.delay(.seconds(1)).sink { ... }
    /// ```
    public func delay(_ duration: Duration) -> AsyncRay<T> {
        chained { continuation, stream in
            // Reading is decoupled from sleeping so a sleep never postpones stamping later values.
            // The in-flight queue holds only values not yet due, i.e. at most `duration` worth of input.
            let (pending, pendingContinuation) = AsyncStream<(ContinuousClock.Instant, T)>.makeStream()
            let reader = Task {
                for await value in stream {
                    pendingContinuation.yield((ContinuousClock.now.advanced(by: duration), value))
                }
                pendingContinuation.finish()
            }
            let writer = Task {
                for await (deadline, value) in pending {
                    do {
                        try await Task.sleep(until: deadline, clock: .continuous)
                    } catch {
                        return  // Cancelled — drop in-flight values
                    }
                    continuation.yield(value)
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in
                reader.cancel()
                writer.cancel()
            }
        }
    }

    // MARK: - debounce

    /// Waits for a quiet window of `duration` and emits the **latest** value.
    ///
    /// Ideal for search fields and rapid user input.
    /// ```swift
    /// searchField.textAsyncRay
    ///     .debounce(.milliseconds(300))
    ///     .flatMapLatest(bufferingPolicy: .bufferingNewest(1)) { query in api.search(query) }
    /// ```
    public func debounce(_ duration: Duration) -> AsyncRay<T> {
        chained { continuation, stream in
            let taskBox = TaskBox<Void>()
            let outerTask = Task {
                for await value in stream {
                    taskBox.replace(with: Task {
                        do {
                            try await Task.sleep(for: duration)
                            guard !Task.isCancelled else { return }
                            continuation.yield(value)
                        } catch {
                            // Task cancelled — do not emit
                        }
                    })
                }
                // Await last pending debounce before regular completion
                await taskBox.snapshot()?.value
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in
                outerTask.cancel()
                taskBox.cancelCurrent()
            }
        }
    }

    // MARK: - throttle

    /// Limits emissions to at most once per `duration` window (leading throttle).
    ///
    /// Forwards the **first** value in each window and drops subsequent values until the window elapses.
    ///
    /// ```swift
    /// buttonTap.asyncRay.throttle(.milliseconds(500)).sink { handleTap() }
    /// ```
    public func throttle(_ duration: Duration) -> AsyncRay<T> {
        chained { continuation, stream in
            let task = Task {
                var lastEmit: ContinuousClock.Instant? = nil
                for await value in stream {
                    let now = ContinuousClock.now
                    if let last = lastEmit, now - last < duration {
                        continue  // within throttle window — skip
                    }
                    lastEmit = now
                    continuation.yield(value)
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }

    // MARK: - timeout

    /// Completes the stream if no value is received within `duration`.
    ///
    /// ```swift
    /// networkAsyncRay.timeout(.seconds(30)).sink(
    ///     next: { handleData($0) },
    ///     completed: { handleTimeout() }
    /// )
    /// ```
    public func timeout(_ duration: Duration) -> AsyncRay<T> {
        chained { continuation, stream in
            let watchdogBox = TaskBox<Void>()
            func armWatchdog() {
                watchdogBox.replace(with: Task {
                    try? await Task.sleep(for: duration)
                    if !Task.isCancelled { continuation.finish() }
                })
            }

            let outerTask = Task {
                armWatchdog()
                for await value in stream {
                    watchdogBox.cancelCurrent()
                    continuation.yield(value)
                    armWatchdog()
                }
                watchdogBox.cancelCurrent()
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in
                outerTask.cancel()
                watchdogBox.cancelCurrent()
            }
        }
    }
}
