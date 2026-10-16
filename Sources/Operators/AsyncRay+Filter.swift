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
        chainedTask { stream, continuation in
            for await value in stream {
                if predicate(value) { continuation.yield(value) }
            }
        }
    }

    // MARK: - take / skip

    /// Takes the first `count` values and completes.
    public func take(_ count: Int) -> AsyncRay<T> {
        guard count > 0 else { return .empty() }
        return chainedTask { stream, continuation in
            var taken = 0
            for await value in stream {
                continuation.yield(value)
                taken += 1
                if taken >= count { break }
            }
        }
    }

    /// Skips the first `count` values.
    public func skip(_ count: Int) -> AsyncRay<T> {
        guard count > 0 else { return self }
        return chainedTask { stream, continuation in
            var skipped = 0
            for await value in stream {
                if skipped < count { skipped += 1; continue }
                continuation.yield(value)
            }
        }
    }

    // MARK: - prefix / drop while

    /// Emits values as long as the predicate evaluates to `true`, then completes.
    public func prefix(while predicate: @Sendable @escaping (T) -> Bool) -> AsyncRay<T> {
        chainedTask { stream, continuation in
            for await value in stream {
                guard predicate(value) else { break }
                continuation.yield(value)
            }
        }
    }

    /// Drops values as long as the predicate evaluates to `true`, then forwards all remaining values.
    public func drop(while predicate: @Sendable @escaping (T) -> Bool) -> AsyncRay<T> {
        chainedTask { stream, continuation in
            var dropping = true
            for await value in stream {
                if dropping && predicate(value) { continue }
                dropping = false
                continuation.yield(value)
            }
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
        skipRepeats { $0 == $1 }
    }
}

extension AsyncRay {

    /// Drops a value when `areEquivalent` returns `true` for it and the previously forwarded value.
    ///
    /// ```swift
    /// users.asyncRay.skipRepeats { $0.id == $1.id }
    /// ```
    public func skipRepeats(
        by areEquivalent: @Sendable @escaping (T, T) -> Bool
    ) -> AsyncRay<T> {
        chainedTask { stream, continuation in
            var last: T? = nil
            for await value in stream {
                if let last, areEquivalent(last, value) { continue }
                last = value
                continuation.yield(value)
            }
        }
    }
}
