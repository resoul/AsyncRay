// Core reactive cold stream backed by AsyncStream

import Foundation

/// A cold reactive stream of values of type `T`.
///
/// **Cold** means each `.sink()` call or access to `.stream` creates a fresh,
/// independent subscription and executes the underlying producer closure.
///
/// ## Creation
/// ```swift
/// // From fixed values
/// let asyncRay = AsyncRay.just(42)
/// let asyncRay = AsyncRay.from([1, 2, 3])
///
/// // Custom producer
/// let asyncRay = AsyncRay<Int> { emitter in
///     emitter.send(1)
///     Task {
///         try? await Task.sleep(for: .seconds(1))
///         emitter.send(2)
///         emitter.finish()
///     }
/// }
/// ```
///
/// ## Subscription
/// ```swift
/// let sub = asyncRay
///     .map { $0 * 2 }
///     .filter { $0 > 0 }
///     .sinkOnMain { value in
///         print(value)
///     }
/// sub.store(in: bag)
/// ```
///
/// ## Native async
/// ```swift
/// for await value in asyncRay.stream { print(value) }
/// let first = await asyncRay.first()
/// ```
///
/// ## Order guarantees
///
/// | Category | Operators | Guarantee |
/// | --- | --- | --- |
/// | Preserves relative order | `map`, `filter`, `compactMap`, `take`, `skip`, `prefix`, `drop`, `then`, `skipRepeats`, `scan` | Output follows upstream order. |
/// | Time-filtered | `debounce`, `throttle` | Relative order of delivered values is preserved, but intermediate values may be dropped. |
/// | Multi-source concurrent | `merge`, `flatMap`, `combineLatest`, `zip` | Delivery depends on task completion and concurrency runtime scheduling. |
/// | Time-shifted | `delay` | Order and spacing are preserved; every value is shifted by the same duration. |
/// | Switch-to-latest | `flatMapLatest` | Previous inner stream is cancelled when a new outer value arrives. Output completes after the outer and the latest inner complete. |
public struct AsyncRay<T: Sendable>: Sendable {

    // Factory: invoked on each .sink() / .stream access.
    // Returns a new AsyncStream instance, ensuring cold stream semantics.
    internal let _make: @Sendable () -> AsyncStream<T>

    /// Inherited buffering policy of a hot upstream source, if known.
    /// Finite cold sources (`from`, `just`) leave this as nil to preserve lossless semantics.
    internal let inheritedBufferingPolicy: _AsyncRayBufferingPolicy?

    // MARK: - Initializers

    /// Creates a new root stream from an `AsyncStream` factory.
    ///
    /// - Warning: This initializer creates a fresh source and intentionally resets any inherited buffering policy.
    ///   Custom downstream operators must use `chained` or internal initializers with explicit/inherited policy.
    public init(_ make: @Sendable @escaping () -> AsyncStream<T>) {
        self._make = make
        self.inheritedBufferingPolicy = nil
    }

    internal init(
        bufferingPolicy: AsyncStream<T>.Continuation.BufferingPolicy,
        _ make: @Sendable @escaping () -> AsyncStream<T>
    ) {
        self._make = make
        self.inheritedBufferingPolicy = _AsyncRayBufferingPolicy(bufferingPolicy)
    }

    internal init(
        inheriting bufferingPolicy: _AsyncRayBufferingPolicy?,
        _ make: @Sendable @escaping () -> AsyncStream<T>
    ) {
        self._make = make
        self.inheritedBufferingPolicy = bufferingPolicy
    }

    /// Creates a stream using `AsyncRayEmitter` for custom generators.
    ///
    /// The `build` closure is executed synchronously upon each subscription.
    /// Use asynchronous `Task {}` blocks inside if needed.
    public init(_ build: @Sendable @escaping (AsyncRayEmitter<T>) -> Void) {
        self._make = {
            AsyncStream<T> { continuation in
                let emitter = AsyncRayEmitter(continuation: continuation)
                build(emitter)
            }
        }
        self.inheritedBufferingPolicy = nil
    }

    // MARK: - Factory Methods

    /// A stream that emits a single value and completes immediately.
    public static func just(_ value: T) -> AsyncRay<T> {
        AsyncRay { emitter in
            emitter.send(value)
            emitter.finish()
        }
    }

    /// An empty stream that completes immediately without emitting any values.
    public static func empty() -> AsyncRay<T> {
        AsyncRay { emitter in
            emitter.finish()
        }
    }

    /// A stream that never completes and never emits any values.
    public static func never() -> AsyncRay<T> {
        AsyncRay { _ in
            // Continuation is retained without emitting
        }
    }

    /// A stream that emits all elements from a sequence and then completes.
    public static func from(_ values: some Sequence<T> & Sendable) -> AsyncRay<T> {
        AsyncRay { emitter in
            for value in values { emitter.send(value) }
            emitter.finish()
        }
    }

    /// A stream that waits for `duration` and completes without emitting values.
    public static func timer(_ duration: Duration) -> AsyncRay<T> {
        AsyncRay {
            AsyncStream<T> { continuation in
                let task = Task {
                    do {
                        try await Task.sleep(for: duration)
                        guard !Task.isCancelled else { return }
                        continuation.finish()
                    } catch {
                        // Cancellation — do not finish continuation
                    }
                }
                continuation.onTermination = { @Sendable _ in task.cancel() }
            }
        }
    }

    // MARK: - Internal Operator Chaining

    /// Builds a downstream `AsyncRay` preserving the upstream buffering policy unless overridden.
    internal func chained<U: Sendable>(
        overridePolicy: AsyncStream<U>.Continuation.BufferingPolicy? = nil,
        _ build: @Sendable @escaping (AsyncStream<U>.Continuation, AsyncStream<T>) -> Void
    ) -> AsyncRay<U> {
        let policy = overridePolicy.map { _AsyncRayBufferingPolicy($0) } ?? inheritedBufferingPolicy
        return AsyncRay<U>(inheriting: policy) { [_make] in
            makeAsyncRayStream(policy: policy) { continuation in
                build(continuation, _make())
            }
        }
    }

    // MARK: - Subscription

    /// Subscribes to receive stream values.
    ///
    /// Returns a `Subscription` handle. Store it in a `SubscriptionBag` for automatic lifecycle management.
    ///
    /// - Parameter handler: Invoked for each emitted value.
    @discardableResult
    public func sink(
        _ handler: @Sendable @escaping (T) -> Void
    ) -> Subscription {
        let stream = _make()
        let subBox = _SubscriptionBox()
        let task = Task {
            for await value in stream {
                guard !Task.isCancelled else { break }
                handler(value)
            }
            guard !Task.isCancelled else { return }
            subBox.markCompleted()
        }
        let sub = Subscription { task.cancel() }
        subBox.subscription = sub
        return sub
    }

    /// Subscribes with an explicit completion callback.
    @discardableResult
    public func sink(
        next: @Sendable @escaping (T) -> Void,
        completed: @Sendable @escaping () -> Void
    ) -> Subscription {
        let stream = _make()
        let subBox = _SubscriptionBox()
        let task = Task {
            for await value in stream {
                guard !Task.isCancelled else { break }
                next(value)
            }
            guard !Task.isCancelled else { return }
            completed()
            subBox.markCompleted()
        }
        let sub = Subscription { task.cancel() }
        subBox.subscription = sub
        return sub
    }

    /// Subscribes directly on `MainActor`.
    ///
    /// Recommended for UI bindings without extra intermediate thread dispatching.
    @discardableResult @MainActor
    public func sinkOnMain(
        _ handler: @MainActor @escaping (T) -> Void
    ) -> Subscription {
        let stream = _make()
        let subBox = _SubscriptionBox()
        let task = Task { @MainActor in
            for await value in stream {
                guard !Task.isCancelled else { break }
                handler(value)
            }
            guard !Task.isCancelled else { return }
            subBox.markCompleted()
        }
        let sub = Subscription { task.cancel() }
        subBox.subscription = sub
        return sub
    }

    // MARK: - Native Async

    /// Accesses the underlying `AsyncStream` for use in `for await` loops.
    ///
    /// Each access creates a new independent subscription.
    public var stream: AsyncStream<T> {
        _make()
    }

    /// Awaits and returns the first emitted value (or `nil` if the stream completed empty).
    public func first() async -> T? {
        for await value in _make() {
            return value
        }
        return nil
    }

    /// Collects all values of a finite stream into an array.
    ///
    /// - Warning: Do not call on infinite streams.
    public func collect() async -> [T] {
        var result: [T] = []
        for await value in _make() {
            result.append(value)
        }
        return result
    }
}

private final class _SubscriptionBox: @unchecked Sendable {
    private let lock = NSLock()
    private var _subscription: Subscription?
    private var _completed = false

    var subscription: Subscription? {
        get { lock.withLock { _subscription } }
        set {
            let shouldMark: Bool = lock.withLock {
                _subscription = newValue
                return _completed
            }
            if shouldMark {
                newValue?.markCompleted()
            }
        }
    }

    func markCompleted() {
        let sub: Subscription? = lock.withLock {
            _completed = true
            return _subscription
        }
        sub?.markCompleted()
    }
}

/// Creates an intermediate stream preserving the bounded policy of a hot source.
/// Cold sources pass nil and retain lossless unbounded semantics.
internal enum _AsyncRayBufferingPolicy: Sendable, Equatable {
    case unbounded
    case bufferingOldest(Int)
    case bufferingNewest(Int)

    init<Element>(_ policy: AsyncStream<Element>.Continuation.BufferingPolicy) {
        switch policy {
        case .unbounded: self = .unbounded
        case .bufferingOldest(let count): self = .bufferingOldest(count)
        case .bufferingNewest(let count): self = .bufferingNewest(count)
        @unknown default: self = .unbounded
        }
    }

    func streamPolicy<Element>() -> AsyncStream<Element>.Continuation.BufferingPolicy {
        switch self {
        case .unbounded: return .unbounded
        case .bufferingOldest(let count): return .bufferingOldest(count)
        case .bufferingNewest(let count): return .bufferingNewest(count)
        }
    }
}

internal func makeAsyncRayStream<Element: Sendable>(
    policy: _AsyncRayBufferingPolicy?,
    _ build: @escaping (AsyncStream<Element>.Continuation) -> Void
) -> AsyncStream<Element> {
    if let policy {
        return AsyncStream<Element>(bufferingPolicy: policy.streamPolicy()) { continuation in build(continuation) }
    }
    return AsyncStream<Element> { continuation in build(continuation) }
}
