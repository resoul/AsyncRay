/// Interface for sending values, failing, and managing lifecycle from a custom generator.
///
/// Used inside `AsyncThrowingRay.init(_ build:)`:
/// ```swift
/// let asyncRay = AsyncThrowingRay<Int> { emitter in
///     emitter.send(1)
///     emitter.fail(NetworkError.disconnected)
/// }
/// ```
public final class AsyncThrowingRayEmitter<T: Sendable>: Sendable {

    private let continuation: AsyncThrowingStream<T, Error>.Continuation
    private let storage = TerminationStorage<
        AsyncThrowingStream<T, Error>.Continuation.Termination
    >()

    internal init(continuation: AsyncThrowingStream<T, Error>.Continuation) {
        self.continuation = continuation
        // Configured once here; later handler registrations are stored in `storage`.
        continuation.onTermination = { @Sendable [storage] reason in
            storage.terminate(reason)
        }
    }

    /// Sends the next value to the subscriber.
    public func send(_ value: T) {
        continuation.yield(value)
    }

    /// Completes the stream normally. No further values will be delivered.
    public func finish() {
        continuation.finish()
    }

    /// Fails the stream with `error`. No further values will be delivered.
    public func fail(_ error: Error) {
        continuation.finish(throwing: error)
    }

    /// Registers a handler for any termination reason: `.finished(nil)` after `finish()`,
    /// `.finished(error)` after `fail(_:)`, or `.cancelled`.
    ///
    /// If the subscription is already terminated by the time this is called,
    /// the handler is invoked immediately with the recorded reason.
    public func onTermination(
        _ handler:
            @Sendable @escaping (AsyncThrowingStream<T, Error>.Continuation.Termination) ->
            Void
    ) {
        storage.add(handler)
    }

    /// Registers a callback invoked only when the subscription is cancelled.
    public func onCancellation(_ handler: @Sendable @escaping () -> Void) {
        storage.add { reason in
            if case .cancelled = reason {
                handler()
            }
        }
    }

    /// Registers a callback invoked on completion, failure, or cancellation.
    public func onFinishOrCancel(_ handler: @Sendable @escaping () -> Void) {
        storage.add { _ in handler() }
    }
}
