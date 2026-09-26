/// Interface for sending values and managing lifecycle from a custom generator.
///
/// Used inside `AsyncRay.init(_ build:)`:
/// ```swift
/// let asyncRay = AsyncRay<Int> { emitter in
///     emitter.send(1)
///     emitter.send(2)
///     emitter.finish()
/// }
/// ```
public final class AsyncRayEmitter<T: Sendable>: Sendable {

    private let continuation: AsyncStream<T>.Continuation
    private let storage = TerminationStorage<T>()

    internal init(continuation: AsyncStream<T>.Continuation) {
        self.continuation = continuation
        // Configured once here. All subsequent onTermination/onCancellation/onFinishOrCancel
        // calls register handlers in `storage` without overwriting continuation.onTermination.
        continuation.onTermination = { @Sendable [storage] reason in
            storage.terminate(reason)
        }
    }

    /// Sends the next value to subscribers.
    public func send(_ value: T) {
        continuation.yield(value)
    }

    /// Completes the stream. No further values will be delivered.
    public func finish() {
        continuation.finish()
    }

    /// Registers a handler for any termination reason (`.finished` or `.cancelled`).
    ///
    /// If the subscription is already terminated by the time this is called,
    /// the handler is invoked immediately with the recorded reason.
    public func onTermination(
        _ handler: @Sendable @escaping (AsyncStream<T>.Continuation.Termination) -> Void
    ) {
        storage.add(handler)
    }

    /// Registers a callback invoked only when the subscription is cancelled.
    ///
    /// Useful for resource cleanup in custom producers.
    ///
    /// ```swift
    /// let asyncRay = AsyncRay<Data> { emitter in
    ///     let connection = openConnection()
    ///     emitter.onCancellation { connection.close() }
    ///     connection.onData { emitter.send($0) }
    /// }
    /// ```
    public func onCancellation(_ handler: @Sendable @escaping () -> Void) {
        storage.add { reason in
            if case .cancelled = reason {
                handler()
            }
        }
    }

    /// Registers a callback invoked on either normal completion (`.finished`) or cancellation (`.cancelled`).
    public func onFinishOrCancel(_ handler: @Sendable @escaping () -> Void) {
        storage.add { _ in handler() }
    }
}
