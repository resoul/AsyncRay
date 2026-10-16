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
        chainedTask { stream, continuation in
            var current = initial
            for await value in stream {
                current = transform(current, value)
                continuation.yield(current)
            }
        }
    }

    // MARK: - chunks

    /// Groups values into arrays of `count` elements.
    ///
    /// When upstream completes, any remaining values are emitted as a final, shorter chunk.
    /// Cancellation drops an incomplete chunk.
    ///
    /// ```swift
    /// AsyncRay.from([1, 2, 3, 4, 5]).chunks(ofCount: 2).collect()  // [[1, 2], [3, 4], [5]]
    /// ```
    public func chunks(ofCount count: Int) -> AsyncRay<[T]> {
        precondition(count > 0, "chunks(ofCount:) requires count > 0")
        return chainedTask { stream, continuation in
            var chunk: [T] = []
            chunk.reserveCapacity(count)
            for await value in stream {
                chunk.append(value)
                if chunk.count == count {
                    continuation.yield(chunk)
                    chunk.removeAll(keepingCapacity: true)
                }
            }
            guard !Task.isCancelled, !chunk.isEmpty else { return }
            continuation.yield(chunk)
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
        chainedTask { stream, continuation in
            var current = initial
            for await value in stream {
                current = transform(current, value)
            }
            guard !Task.isCancelled else { return }
            continuation.yield(current)
        }
    }
}
