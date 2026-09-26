// MARK: - AsyncStream → AsyncRay

extension AsyncStream where Element: Sendable {
    /// Converts an `AsyncStream` into a `AsyncRay`.
    ///
    /// `AsyncStream` is a single-consumer stream. This method creates a lazy, shared bridge
    /// that multicasts values to all active subscribers and automatically cancels the upstream pump
    /// when the last subscriber disconnects (0 -> 1 -> 0).
    /// The bridge is one-shot: when the upstream finishes or its last subscriber disconnects,
    /// it becomes terminal. Subscribers created afterward receive an already-finished stream.
    /// Each subscriber has its own buffer using `bufferingPolicy`.
    ///
    /// ```swift
    /// stream.asAsyncRay()
    ///     .compactMap { $0.asMessage }
    ///     .sinkOnMain { appendMessage($0) }
    /// ```
    public func asAsyncRay(
        bufferingPolicy: AsyncStream<Element>.Continuation.BufferingPolicy = .bufferingNewest(64)
    ) -> AsyncRay<Element> {
        let shared = SharedAsyncRay(self, bufferingPolicy: bufferingPolicy)
        return shared.asyncRay
    }
}

// MARK: - AsyncRay → AsyncStream

extension AsyncRay: AsyncSequence {
    /// The element type produced by this sequence.
    public typealias Element = T

    /// The iterator type of the fresh stream created for each iteration.
    public typealias AsyncIterator = AsyncStream<T>.AsyncIterator

    /// Creates an iterator for a new independent subscription.
    public func makeAsyncIterator() -> AsyncStream<T>.AsyncIterator {
        _make().makeAsyncIterator()
    }

    /// Converts into an `AsyncThrowingStream` for interoperability with throwing async APIs.
    /// The current `AsyncRay` API has no failure channel, so this stream finishes normally;
    /// cancelling its iteration cancels the forwarding task and upstream subscription.
    public var throwingStream: AsyncThrowingStream<T, Error> {
        AsyncThrowingStream { continuation in
            let task = Task {
                for await value in _make() {
                    continuation.yield(value)
                }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }
}

// MARK: - AsyncRay.Merge with AsyncStream

extension AsyncRay {
    /// Merges this `AsyncRay` with an `AsyncStream` of the same element type using an explicit
    /// output buffering policy. The `AsyncStream` is consumed by the shared bridge, so it is
    /// not restarted for each subscription to the returned `AsyncRay`.
    public func merge(
        with stream: AsyncStream<T>,
        bufferingPolicy: AsyncStream<T>.Continuation.BufferingPolicy
    ) -> AsyncRay<T> {
        merge(
            with: stream.asAsyncRay(bufferingPolicy: bufferingPolicy),
            bufferingPolicy: bufferingPolicy
        )
    }
}
