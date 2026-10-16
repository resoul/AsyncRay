import Foundation

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
        delay(duration, clock: ContinuousClock())
    }

    /// Shifts the whole timeline by `duration`, measured by `clock`.
    ///
    /// Behaves like `delay(_:)`; pass a custom clock (for example a manual test clock) to
    /// control time deterministically.
    public func delay<C: Clock>(_ duration: Duration, clock: C) -> AsyncRay<T>
    where C.Duration == Duration {
        chained { continuation, stream in
            // Reading is decoupled from sleeping so a sleep never postpones stamping later values.
            // The in-flight queue holds only values not yet due, i.e. at most `duration` worth of input.
            let (pending, pendingContinuation) = AsyncStream<(C.Instant, T)>
                .makeStream()
            let reader = Task {
                for await value in stream {
                    pendingContinuation.yield((clock.now.advanced(by: duration), value))
                }
                pendingContinuation.finish()
            }
            let writer = Task {
                for await (deadline, value) in pending {
                    do {
                        try await clock.sleep(until: deadline, tolerance: nil)
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
    /// A pending value is emitted after the quiet window even if upstream completes during
    /// that window. Cancellation drops the pending value.
    /// ```swift
    /// searchField.textAsyncRay
    ///     .debounce(.milliseconds(300))
    ///     .flatMapLatest(bufferingPolicy: .bufferingNewest(1)) { query in api.search(query) }
    /// ```
    public func debounce(_ duration: Duration) -> AsyncRay<T> {
        debounce(duration, clock: ContinuousClock())
    }

    /// Waits for a quiet window of `duration`, measured by `clock`, and emits the latest value.
    ///
    /// Behaves like `debounce(_:)`; pass a custom clock to control time deterministically.
    public func debounce<C: Clock>(_ duration: Duration, clock: C) -> AsyncRay<T>
    where C.Duration == Duration {
        chained { continuation, stream in
            let taskBox = TaskBox<Void>()
            let outerTask = Task {
                for await value in stream {
                    taskBox.replace(
                        with: Task {
                            do {
                                try await clock.sleep(for: duration)
                                guard !Task.isCancelled else { return }
                                continuation.yield(value)
                            } catch {
                                // Task cancelled — do not emit
                            }
                        }
                    )
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
        throttle(duration, clock: ContinuousClock())
    }

    /// Forwards the first value in each `duration` window, measured by `clock`.
    ///
    /// Behaves like `throttle(_:)`; pass a custom clock to control time deterministically.
    public func throttle<C: Clock>(_ duration: Duration, clock: C) -> AsyncRay<T>
    where C.Duration == Duration {
        chainedTask { stream, continuation in
            var lastEmit: C.Instant? = nil
            for await value in stream {
                let now = clock.now
                if let last = lastEmit, last.duration(to: now) < duration {
                    continue  // within throttle window — skip
                }
                lastEmit = now
                continuation.yield(value)
            }
        }
    }

    /// Limits emissions to at most once per `duration` window, choosing which value to forward.
    ///
    /// With `latest: false` this is the leading throttle of `throttle(_:)`. With `latest: true`
    /// the first value is forwarded immediately and opens a window; at the end of the window the
    /// **latest** value received during it, if any, is forwarded and opens the next window.
    /// A value still pending when upstream completes is delivered at the end of its window
    /// before the output completes. Cancellation drops it.
    ///
    /// ```swift
    /// scrollOffset.asyncRay.throttle(.milliseconds(100), latest: true).sinkOnMain { update($0) }
    /// ```
    public func throttle(_ duration: Duration, latest: Bool) -> AsyncRay<T> {
        throttle(duration, latest: latest, clock: ContinuousClock())
    }

    /// Limits emissions to at most once per `duration` window, measured by `clock`.
    ///
    /// Behaves like `throttle(_:latest:)`; pass a custom clock to control time deterministically.
    public func throttle<C: Clock>(_ duration: Duration, latest: Bool, clock: C) -> AsyncRay<T>
    where C.Duration == Duration {
        guard latest else { return throttle(duration, clock: clock) }
        return chained { continuation, stream in
            let window = _ThrottleWindow<T>()
            let windowTask = TaskBox<Void>()
            // One window task at a time: it is started only by a value that found the window
            // closed, and it closes the window only when it has nothing left to emit.
            @Sendable func openWindow() {
                windowTask.replace(
                    with: Task {
                        while true {
                            do {
                                try await clock.sleep(for: duration)
                            } catch {
                                return  // Cancelled — drop the pending value
                            }
                            guard let value = window.takePendingOrClose() else { return }
                            continuation.yield(value)
                        }
                    }
                )
            }

            let outerTask = Task {
                for await value in stream {
                    if window.offer(value) {
                        continuation.yield(value)
                        openWindow()
                    }
                }
                // Deliver a value still pending in the current window before completing.
                await windowTask.snapshot()?.value
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in
                outerTask.cancel()
                windowTask.cancelCurrent()
            }
        }
    }

    // MARK: - timeout

    /// Completes the stream if no value is received within `duration`.
    /// The timer starts when the subscription begins and resets after each value. A quiet
    /// timeout completes normally without an error; values received after the timeout are not
    /// forwarded. Normal upstream completion also completes the output.
    ///
    /// ```swift
    /// networkAsyncRay.timeout(.seconds(30)).sink(
    ///     next: { handleData($0) },
    ///     completed: { handleTimeout() }
    /// )
    /// ```
    public func timeout(_ duration: Duration) -> AsyncRay<T> {
        timeout(duration, clock: ContinuousClock())
    }

    /// Completes the stream if no value is received within `duration`, measured by `clock`.
    ///
    /// Behaves like `timeout(_:)`; pass a custom clock to control time deterministically.
    public func timeout<C: Clock>(_ duration: Duration, clock: C) -> AsyncRay<T>
    where C.Duration == Duration {
        chained { continuation, stream in
            let watchdogBox = TaskBox<Void>()
            func armWatchdog() {
                watchdogBox.replace(
                    with: Task {
                        try? await clock.sleep(for: duration)
                        if !Task.isCancelled { continuation.finish() }
                    }
                )
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

// MARK: - interval

extension AsyncRay where T == Int {

    /// A stream that emits `0, 1, 2, ...` every `period`, starting one `period` after
    /// subscription, and never completes.
    ///
    /// Tick deadlines are fixed multiples of `period` from the start, so a slow tick does not
    /// shift later ones. The stream keeps only the newest tick for a slow consumer, so tick
    /// numbers may skip; it never builds a backlog. Cancel the subscription to stop it.
    ///
    /// ```swift
    /// AsyncRay.interval(.seconds(30)).sink { _ in sendHeartbeat() }.store(in: bag)
    /// ```
    public static func interval(_ period: Duration) -> AsyncRay<Int> {
        interval(period, clock: ContinuousClock())
    }

    /// A stream that emits `0, 1, 2, ...` every `period`, measured by `clock`.
    ///
    /// Behaves like `interval(_:)`; pass a custom clock to control time deterministically.
    public static func interval<C: Clock>(_ period: Duration, clock: C) -> AsyncRay<Int>
    where C.Duration == Duration {
        precondition(period > .zero, "interval requires a positive period")
        return AsyncRay(bufferingPolicy: .bufferingNewest(1)) {
            AsyncStream<Int>(bufferingPolicy: .bufferingNewest(1)) { continuation in
                let task = Task {
                    let start = clock.now
                    var tick = 0
                    while true {
                        do {
                            try await clock.sleep(
                                until: start.advanced(by: period * (tick + 1)),
                                tolerance: nil
                            )
                        } catch {
                            return  // Cancelled
                        }
                        continuation.yield(tick)
                        tick += 1
                    }
                }
                continuation.onTermination = { @Sendable _ in task.cancel() }
            }
        }
    }
}

/// Window state for `throttle(_:latest:clock:)`, shared by the reading task and the window task.
private final class _ThrottleWindow<T: Sendable>: @unchecked Sendable {
    private let lock = NSLock()
    private var isOpen = false
    private var pending: T?

    /// Returns `true` when the window was closed: the caller forwards `value` and opens a window.
    /// Otherwise `value` replaces the pending value of the open window.
    func offer(_ value: T) -> Bool {
        lock.withLock {
            if isOpen {
                pending = value
                return false
            }
            isOpen = true
            return true
        }
    }

    /// Takes the pending value, keeping the window open for it, or closes the window.
    func takePendingOrClose() -> T? {
        lock.withLock {
            guard let value = pending else {
                isOpen = false
                return nil
            }
            pending = nil
            return value
        }
    }
}
