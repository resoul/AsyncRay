import Foundation

// Ensures explicit cancellation linkage between the outer AsyncStream task and nested tasks,
// preventing leaked background sleep loops or delivery after subscription cancellation.

internal final class TaskBox<Success: Sendable>: @unchecked Sendable {

    private let lock = NSLock()
    private var current: Task<Success, Never>?

    init() {}

    /// Atomically replaces the current task with a new one, cancelling the previous task.
    @discardableResult
    func replace(with newTask: Task<Success, Never>) -> Task<Success, Never>? {
        let previous: Task<Success, Never>? = lock.withLock {
            let old = current
            current = newTask
            return old
        }
        previous?.cancel()
        return previous
    }

    /// Takes a snapshot of the current task to await its completion without race conditions.
    func snapshot() -> Task<Success, Never>? {
        lock.withLock { current }
    }

    /// Cancels and clears the current task if present.
    func cancelCurrent() {
        let task: Task<Success, Never>? = lock.withLock {
            let old = current
            current = nil
            return old
        }
        task?.cancel()
    }
}
