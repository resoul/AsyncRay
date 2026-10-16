import Foundation

// Solves the late-registration race: when a producer registers cleanup after an `await` point
// (e.g. opening a network connection), the subscription may have already been cancelled.
// TerminationStorage records the terminal reason and immediately replays it to any newly registered handlers.

/// Thread-safe storage for termination handlers.
///
/// - `add`: Registers a handler. If the terminal reason is already recorded, the handler is invoked immediately.
/// - `terminate`: Records the terminal reason (idempotently) and invokes all pending handlers outside the lock.
internal final class TerminationStorage<Reason: Sendable>: @unchecked Sendable {

    private let lock = NSLock()
    private var termination: Reason?
    private var handlers: [@Sendable (Reason) -> Void] = []

    init() {}

    /// Registers a handler. Invoked exactly once: either later upon `terminate(_:)`
    /// or immediately if termination has already occurred.
    func add(_ handler: @escaping @Sendable (Reason) -> Void) {
        let alreadyTerminated: Reason? = lock.withLock {
            if let termination {
                return termination
            }
            handlers.append(handler)
            return nil
        }
        // Invoked outside lock to prevent re-entrancy or deadlocks from user closures
        if let alreadyTerminated {
            handler(alreadyTerminated)
        }
    }

    /// Records the terminal reason and invokes all accumulated handlers.
    /// Subsequent calls are no-ops (idempotent).
    func terminate(_ reason: Reason) {
        let snapshot: [@Sendable (Reason) -> Void] = lock.withLock {
            guard termination == nil else { return [] }
            termination = reason
            let copy = handlers
            handlers = []
            return copy
        }
        snapshot.forEach { $0(reason) }
    }
}
