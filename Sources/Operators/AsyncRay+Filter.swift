extension AsyncRay {

    // MARK: - filter

    /// Forwards only values matching the predicate.
    ///
    /// ```swift
    /// connectionState.asyncRay.filter { $0 == .connected }.sink { ... }
    /// ```
    public func filter(
        _ predicate: @Sendable @escaping (T) -> Bool
    ) -> AsyncRay<T> {
        chained { continuation, stream in
            let task = Task {
                for await value in stream {
                    if predicate(value) { continuation.yield(value) }
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }

    // MARK: - take / skip

    /// Takes the first `count` values and completes.
    public func take(_ count: Int) -> AsyncRay<T> {
        guard count > 0 else { return .empty() }
        return chained { continuation, stream in
            let task = Task {
                var taken = 0
                for await value in stream {
                    continuation.yield(value)
                    taken += 1
                    if taken >= count { break }
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }

    /// Skips the first `count` values.
    public func skip(_ count: Int) -> AsyncRay<T> {
        guard count > 0 else { return self }
        return chained { continuation, stream in
            let task = Task {
                var skipped = 0
                for await value in stream {
                    if skipped < count { skipped += 1; continue }
                    continuation.yield(value)
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }

    // MARK: - prefix / drop while

    /// Emits values as long as the predicate evaluates to `true`, then completes.
    public func prefix(while predicate: @Sendable @escaping (T) -> Bool) -> AsyncRay<T> {
        chained { continuation, stream in
            let task = Task {
                for await value in stream {
                    guard predicate(value) else { break }
                    continuation.yield(value)
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }

    /// Drops values as long as the predicate evaluates to `true`, then forwards all remaining values.
    public func drop(while predicate: @Sendable @escaping (T) -> Bool) -> AsyncRay<T> {
        chained { continuation, stream in
            let task = Task {
                var dropping = true
                for await value in stream {
                    if dropping && predicate(value) { continue }
                    dropping = false
                    continuation.yield(value)
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }
}

// MARK: - skipRepeats (requires Equatable)

extension AsyncRay where T: Equatable {

    /// Drops consecutive duplicate values.
    ///
    /// ```swift
    /// AsyncRay.from([1, 1, 2, 2, 3]).skipRepeats().collect()  // [1, 2, 3]
    /// ```
    public func skipRepeats() -> AsyncRay<T> {
        chained { continuation, stream in
            let task = Task {
                var last: T? = nil
                for await value in stream {
                    guard value != last else { continue }
                    last = value
                    continuation.yield(value)
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }
}
