// Hot push-based multicast event source

import Foundation

/// A hot event source that broadcasts values to all active subscribers.
///
/// **Hot** means values are sent immediately to whoever is currently subscribed.
/// A new subscriber does not receive previously emitted values.
///
/// For state retention and replay, use `CurrentValue`.
public final class Pipe<T: Sendable>: @unchecked Sendable {

    private let lock = NSLock()
    private var continuations: [UUID: AsyncStream<T>.Continuation] = [:]
    private var isFinished = false
    private let bufferingPolicy: AsyncStream<T>.Continuation.BufferingPolicy

    /// - Parameter bufferingPolicy: Buffering policy applied per subscriber.
    ///   Defaults to `.bufferingNewest(64)` to bound memory usage on slow consumers.
    public init(
        bufferingPolicy: AsyncStream<T>.Continuation.BufferingPolicy = .bufferingNewest(64)
    ) {
        self.bufferingPolicy = bufferingPolicy
    }

    // MARK: - Push

    /// Sends a value to all active subscribers.
    public func send(_ value: T) {
        _ = sendObservingOverflow(value)
    }

    /// Sends a value to all active subscribers and returns a `YieldResult` per subscriber
    /// for telemetry or controlled reconnects on buffer overflows.
    @discardableResult
    public func sendObservingOverflow(_ value: T) -> [AsyncStream<T>.Continuation.YieldResult] {
        let all = lock.withLock { () -> [AsyncStream<T>.Continuation] in
            guard !isFinished else { return [] }
            return Array(continuations.values)
        }
        return all.map { $0.yield(value) }
    }

    /// Completes the stream. All active subscribers receive a completion signal.
    public func finish() {
        let all = lock.withLock { () -> [AsyncStream<T>.Continuation] in
            guard !isFinished else { return [] }
            isFinished = true
            let copy = Array(continuations.values)
            continuations = [:]
            return copy
        }
        all.forEach { $0.finish() }
    }

    // MARK: - Subscription

    /// A `AsyncRay` stream for subscribing to broadcast events.
    ///
    /// Each subscription creates an independent subscriber.
    public var asyncRay: AsyncRay<T> {
        AsyncRay(bufferingPolicy: bufferingPolicy) { [weak self, bufferingPolicy] in
            AsyncStream<T>(bufferingPolicy: bufferingPolicy) { continuation in
                guard let self else {
                    continuation.finish()
                    return
                }
                let id = UUID()
                let finished = self.lock.withLock { () -> Bool in
                    if self.isFinished { return true }
                    self.continuations[id] = continuation
                    return false
                }
                if finished {
                    continuation.finish()
                    return
                }
                continuation.onTermination = { @Sendable [weak self] _ in
                    guard let self else { return }
                    self.lock.lock()
                    self.continuations.removeValue(forKey: id)
                    self.lock.unlock()
                }
            }
        }
    }

    /// Direct `AsyncStream` without `AsyncRay` wrapper.
    public var stream: AsyncStream<T> {
        asyncRay.stream
    }

    /// Count of active subscribers (useful for testing and diagnostics).
    public var subscriberCount: Int {
        lock.withLock { continuations.count }
    }
}
