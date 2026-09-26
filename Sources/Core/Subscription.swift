import Foundation

/// An active subscription handle for `AsyncRay`.
/// Calling `cancel()` requests task cancellation and discards subsequent buffered delivery.
/// A callback already executing (or concurrently admitted) may finish. Cancellation does
/// not undo its effects; UI owners should also validate their session before committing.
public final class Subscription: @unchecked Sendable {

    private enum State {
        case active
        case terminated
    }

    private let lock = NSLock()
    private var state: State = .active
    private let _cancel: @Sendable () -> Void
    private var observers: [@Sendable () -> Void] = []

    /// Unique identifier for this subscription.
    public let id: UUID

    /// Creates a subscription handle.
    ///
    /// - Parameters:
    ///   - id: Identifier used by subscription containers; defaults to a new UUID.
    ///   - cancel: Action invoked once when this subscription is cancelled.
    public init(id: UUID = UUID(), cancel: @Sendable @escaping () -> Void) {
        self.id = id
        self._cancel = cancel
    }

    /// Cancels the subscription. Safe to call multiple times.
    public func cancel() {
        terminate(isCancellation: true)
    }

    /// Marks the subscription as completed upon normal finish of the stream.
    internal func markCompleted() {
        terminate(isCancellation: false)
    }

    /// Registers a termination observer (invoked on cancellation or completion).
    internal func addObserver(_ observer: @Sendable @escaping () -> Void) {
        let alreadyTerminated: Bool = lock.withLock {
            if state == .terminated {
                return true
            }
            observers.append(observer)
            return false
        }
        if alreadyTerminated {
            observer()
        }
    }

    private func terminate(isCancellation: Bool) {
        let (shouldCancel, observersToNotify): (Bool, [@Sendable () -> Void]) = lock.withLock {
            guard state == .active else { return (false, []) }
            state = .terminated
            let obs = observers
            observers = []
            return (isCancellation, obs)
        }
        if shouldCancel {
            _cancel()
        }
        observersToNotify.forEach { $0() }
    }

    /// Adds this subscription to a `SubscriptionBag` for automatic cancellation upon deinit.
    public func store(in bag: SubscriptionBag) {
        bag.add(self)
    }
}
