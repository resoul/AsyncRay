// Stateful stream operators: scan, reduce

extension AsyncRay {

    // MARK: - scan

    /// Computes a running accumulation over emitted elements, emitting each intermediate state.
    ///
    /// For each upstream value, `transform` is called with the current accumulator and upstream value,
    /// and the new accumulator is emitted downstream. State is isolated per subscription.
    ///
    /// ```swift
    /// AsyncRay.from([1, 2, 3])
    ///     .scan(0) { acc, next in acc + next }
    ///     .collect()  // [1, 3, 6]
    /// ```
    public func scan<Accumulator: Sendable>(
        _ initial: Accumulator,
        _ transform: @Sendable @escaping (Accumulator, T) -> Accumulator
    ) -> AsyncRay<Accumulator> {
        chained { continuation, stream in
            let task = Task {
                var current = initial
                for await value in stream {
                    current = transform(current, value)
                    continuation.yield(current)
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }

    // MARK: - reduce

    /// Accumulates all emitted elements and emits the final accumulated result only upon normal completion.
    ///
    /// - Note: If the upstream stream is empty, `reduce` emits `initial` once upon completion (matching Combine and RxSwift).
    /// - Note: If the subscription is cancelled before completion, `reduce` does not emit any value.
    ///
    /// ```swift
    /// AsyncRay.from([1, 2, 3])
    ///     .reduce(0) { acc, next in acc + next }
    ///     .collect()  // [6]
    /// ```
    public func reduce<Accumulator: Sendable>(
        _ initial: Accumulator,
        _ transform: @Sendable @escaping (Accumulator, T) -> Accumulator
    ) -> AsyncRay<Accumulator> {
        chained { continuation, stream in
            let task = Task {
                var current = initial
                for await value in stream {
                    current = transform(current, value)
                }
                guard !Task.isCancelled else { return }
                continuation.yield(current)
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }
}
