import Foundation

/// A cold reactive stream of values of type `T` that can fail with an error.
///
/// `AsyncThrowingRay` is the failable counterpart of `AsyncRay`, built on
/// `AsyncThrowingStream`. Like `AsyncRay` it is **cold**: each subscription runs the producer
/// anew. A stream ends in exactly one of three ways: it completes normally, it fails with an
/// error, or its subscription is cancelled. Cancellation reports neither completion nor failure.
///
/// ## Creation
/// ```swift
/// // One async operation per subscription
/// let user = AsyncThrowingRay.from { try await api.fetchUser(id) }
///
/// // Custom producer
/// let frames = AsyncThrowingRay<Frame> { emitter in
///     let socket = openSocket()
///     emitter.onCancellation { socket.close() }
///     socket.onFrame { emitter.send($0) }
///     socket.onError { emitter.fail($0) }
/// }
///
/// // From a non-failing stream
/// let decoded = pipe.asyncRay.tryMap { try JSONDecoder().decode(Event.self, from: $0) }
/// ```
///
/// ## Handling errors
/// ```swift
/// user
///     .retry(2, delay: .seconds(1))
///     .catch { _ in AsyncRay.just(.guest) }  // back to a non-failing AsyncRay
///     .sinkOnMain { profileView.show($0) }
///     .store(in: bag)
///
/// // Or handle the failure in the subscriber
/// user.sinkOnMain(
///     next: { profileView.show($0) },
///     failure: { errorBanner.show($0) }
/// ).store(in: bag)
/// ```
public struct AsyncThrowingRay<T: Sendable>: Sendable {

    // Factory: invoked on each subscription. Returns a new AsyncThrowingStream instance,
    // ensuring cold stream semantics.
    internal let _make: @Sendable () -> AsyncThrowingStream<T, Error>

    /// Inherited buffering policy of a hot upstream source, if known.
    internal let inheritedBufferingPolicy: _AsyncRayBufferingPolicy?

    // MARK: - Initializers

    /// Creates a new root stream from an `AsyncThrowingStream` factory.
    ///
    /// The factory runs once for each subscription. The returned stream controls its own
    /// buffering and termination behavior.
    public init(_ make: @Sendable @escaping () -> AsyncThrowingStream<T, Error>) {
        self._make = make
        self.inheritedBufferingPolicy = nil
    }

    internal init(
        inheriting bufferingPolicy: _AsyncRayBufferingPolicy?,
        _ make: @Sendable @escaping () -> AsyncThrowingStream<T, Error>
    ) {
        self._make = make
        self.inheritedBufferingPolicy = bufferingPolicy
    }

    /// Creates a stream using `AsyncThrowingRayEmitter` for custom generators.
    ///
    /// The `build` closure is executed synchronously upon each subscription. Values use the
    /// default unbounded buffer. Register cleanup with the emitter for work that must stop when
    /// the subscription is cancelled.
    public init(_ build: @Sendable @escaping (AsyncThrowingRayEmitter<T>) -> Void) {
        self._make = {
            AsyncThrowingStream<T, Error> { continuation in
                build(AsyncThrowingRayEmitter(continuation: continuation))
            }
        }
        self.inheritedBufferingPolicy = nil
    }

    // MARK: - Factory Methods

    /// A stream that runs `operation` once per subscription, emits its result and completes,
    /// or fails with the error it throws.
    ///
    /// The operation starts when a subscription is created and is cancelled with it. An error
    /// thrown after cancellation, such as `CancellationError`, is not reported.
    public static func from(
        _ operation: @Sendable @escaping () async throws -> T
    ) -> AsyncThrowingRay<T> {
        AsyncThrowingRay {
            AsyncThrowingStream<T, Error> { continuation in
                let task = Task {
                    do {
                        let value = try await operation()
                        guard !Task.isCancelled else { return }
                        continuation.yield(value)
                        continuation.finish()
                    } catch {
                        guard !Task.isCancelled else { return }
                        continuation.finish(throwing: error)
                    }
                }
                continuation.onTermination = { @Sendable _ in task.cancel() }
            }
        }
    }

    /// A stream that emits all elements from a sequence and then completes.
    public static func from(_ values: some Sequence<T> & Sendable) -> AsyncThrowingRay<T> {
        AsyncThrowingRay { emitter in
            for value in values { emitter.send(value) }
            emitter.finish()
        }
    }

    /// A stream that emits a single value and completes immediately.
    public static func just(_ value: T) -> AsyncThrowingRay<T> {
        AsyncThrowingRay { emitter in
            emitter.send(value)
            emitter.finish()
        }
    }

    /// A stream that fails immediately with `error` without emitting values.
    public static func fail(_ error: Error) -> AsyncThrowingRay<T> {
        AsyncThrowingRay { emitter in
            emitter.fail(error)
        }
    }

    /// An empty stream that completes immediately without emitting any values.
    public static func empty() -> AsyncThrowingRay<T> {
        AsyncThrowingRay { emitter in
            emitter.finish()
        }
    }

    // MARK: - Internal Operator Chaining

    /// Builds a downstream operator whose `body` consumes upstream on a single task.
    ///
    /// Cancelling downstream cancels the task. When `body` returns, downstream finishes; when it
    /// throws, downstream fails with that error. Neither happens after cancellation.
    internal func chainedTask<U: Sendable>(
        _ body:
            @Sendable @escaping (
                AsyncThrowingStream<T, Error>, AsyncThrowingStream<U, Error>.Continuation
            ) async throws -> Void
    ) -> AsyncThrowingRay<U> {
        let policy = inheritedBufferingPolicy
        return AsyncThrowingRay<U>(inheriting: policy) { [_make] in
            makeAsyncThrowingRayStream(policy: policy) { continuation in
                let task = Task {
                    do {
                        try await body(_make(), continuation)
                        guard !Task.isCancelled else { return }
                        continuation.finish()
                    } catch {
                        guard !Task.isCancelled else { return }
                        continuation.finish(throwing: error)
                    }
                }
                continuation.onTermination = { @Sendable _ in task.cancel() }
            }
        }
    }

    /// Builds a non-failing downstream whose `body` consumes this stream on a single task.
    /// Downstream finishes when `body` returns, unless the task was cancelled.
    internal func chainedTaskToAsyncRay<U: Sendable>(
        _ body:
            @Sendable @escaping (AsyncThrowingStream<T, Error>, AsyncStream<U>.Continuation) async
            -> Void
    ) -> AsyncRay<U> {
        let policy = inheritedBufferingPolicy
        return AsyncRay<U>(inheriting: policy) { [_make] in
            makeAsyncRayStream(policy: policy) { continuation in
                let task = Task {
                    await body(_make(), continuation)
                    guard !Task.isCancelled else { return }
                    continuation.finish()
                }
                continuation.onTermination = { @Sendable _ in task.cancel() }
            }
        }
    }

    // MARK: - Subscription

    /// Subscribes to values, failure, and completion.
    ///
    /// Exactly one of `failure` or `completed` runs when the stream ends on its own; neither
    /// runs after the subscription is cancelled. All callbacks run on the subscription task; use
    /// `sinkOnMain(next:failure:completed:)` for `MainActor` delivery.
    public func sink(
        next: @Sendable @escaping (T) -> Void,
        failure: @Sendable @escaping (Error) -> Void,
        completed: @Sendable @escaping () -> Void = {}
    ) -> Subscription {
        let stream = _make()
        let subBox = _SubscriptionBox()
        let task = Task {
            do {
                for try await value in stream {
                    guard !Task.isCancelled else { return }
                    next(value)
                }
                guard !Task.isCancelled else { return }
                completed()
            } catch {
                guard !Task.isCancelled else { return }
                failure(error)
            }
            subBox.markCompleted()
        }
        let sub = Subscription { task.cancel() }
        subBox.subscription = sub
        return sub
    }

    /// Subscribes on `MainActor` to values, failure, and completion.
    ///
    /// Exactly one of `failure` or `completed` runs when the stream ends on its own; neither
    /// runs after the subscription is cancelled. Cancellation does not interrupt a callback
    /// already running.
    @MainActor
    public func sinkOnMain(
        next: @MainActor @escaping (T) -> Void,
        failure: @MainActor @escaping (Error) -> Void,
        completed: @MainActor @escaping () -> Void = {}
    ) -> Subscription {
        let stream = _make()
        let subBox = _SubscriptionBox()
        let task = Task { @MainActor in
            do {
                for try await value in stream {
                    guard !Task.isCancelled else { return }
                    next(value)
                }
                guard !Task.isCancelled else { return }
                completed()
            } catch {
                guard !Task.isCancelled else { return }
                failure(error)
            }
            subBox.markCompleted()
        }
        let sub = Subscription { task.cancel() }
        subBox.subscription = sub
        return sub
    }

    // MARK: - Native Async

    /// Accesses the underlying `AsyncThrowingStream` for use in `for try await` loops.
    ///
    /// Each access creates a new independent subscription.
    public var stream: AsyncThrowingStream<T, Error> {
        _make()
    }

    /// Awaits and returns the first emitted value, or `nil` if the stream completed empty.
    ///
    /// - Throws: The stream's error if it fails before emitting a value.
    public func first() async throws -> T? {
        for try await value in _make() {
            return value
        }
        return nil
    }

    /// Collects all values of a finite stream into an array.
    ///
    /// - Throws: The stream's error if it fails; values received before the failure are lost.
    /// - Warning: Do not call on infinite streams.
    public func collect() async throws -> [T] {
        var result: [T] = []
        for try await value in _make() {
            result.append(value)
        }
        return result
    }
}

extension AsyncThrowingRay: AsyncSequence {
    /// The element type produced by this sequence.
    public typealias Element = T

    /// The iterator type of the fresh stream created for each iteration.
    public typealias AsyncIterator = AsyncThrowingStream<T, Error>.AsyncIterator

    /// Creates an iterator for a new independent subscription.
    public func makeAsyncIterator() -> AsyncThrowingStream<T, Error>.AsyncIterator {
        _make().makeAsyncIterator()
    }
}

extension _AsyncRayBufferingPolicy {
    func throwingStreamPolicy<Element>()
        -> AsyncThrowingStream<Element, Error>.Continuation.BufferingPolicy
    {
        switch self {
        case .unbounded: return .unbounded
        case .bufferingOldest(let count): return .bufferingOldest(count)
        case .bufferingNewest(let count): return .bufferingNewest(count)
        }
    }
}

internal func makeAsyncThrowingRayStream<Element: Sendable>(
    policy: _AsyncRayBufferingPolicy?,
    _ build: @escaping (AsyncThrowingStream<Element, Error>.Continuation) -> Void
) -> AsyncThrowingStream<Element, Error> {
    if let policy {
        return AsyncThrowingStream<Element, Error>(
            bufferingPolicy: policy.throwingStreamPolicy()
        ) { continuation in
            build(continuation)
        }
    }
    return AsyncThrowingStream<Element, Error> { continuation in build(continuation) }
}
