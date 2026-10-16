import Foundation

/// A clock whose time moves only when a test calls `advance(by:)`.
///
/// Sleepers are resumed in deadline order once `advance(by:)` reaches their deadline.
/// Cancelling a sleeping task resumes it with `CancellationError`, like `Task.sleep`.
final class ManualClock: Clock, @unchecked Sendable {

    struct Instant: InstantProtocol {
        var offset: Duration

        func advanced(by duration: Duration) -> Instant {
            Instant(offset: offset + duration)
        }

        func duration(to other: Instant) -> Duration {
            other.offset - offset
        }

        static func < (lhs: Instant, rhs: Instant) -> Bool {
            lhs.offset < rhs.offset
        }
    }

    private struct Sleeper {
        let deadline: Instant
        let continuation: CheckedContinuation<Void, Error>
    }

    private let lock = NSLock()
    private var _now = Instant(offset: .zero)
    private var sleepers: [UUID: Sleeper] = [:]
    private var _sleepCount = 0
    private var _nowReadCount = 0

    var now: Instant {
        lock.withLock {
            _nowReadCount += 1
            return _now
        }
    }

    /// Number of times `now` has been read; lets a test wait until an operator stamped values.
    var nowReadCount: Int {
        lock.withLock { _nowReadCount }
    }

    var minimumResolution: Duration { .zero }

    /// Total number of `sleep` calls that have registered so far, including finished ones.
    var sleepCount: Int {
        lock.withLock { _sleepCount }
    }

    /// Number of sleepers still waiting for their deadline.
    var pendingSleepers: Int {
        lock.withLock { sleepers.count }
    }

    func sleep(until deadline: Instant, tolerance: Duration? = nil) async throws {
        let id = UUID()
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                enum Registration { case resume, cancel, wait }
                // The cancellation check shares the lock with onCancel, so a sleeper is either
                // registered before onCancel removes it or sees the cancelled flag here.
                let registration: Registration = lock.withLock {
                    _sleepCount += 1
                    if Task.isCancelled { return .cancel }
                    if deadline <= _now { return .resume }
                    sleepers[id] = Sleeper(deadline: deadline, continuation: continuation)
                    return .wait
                }
                switch registration {
                case .resume: continuation.resume()
                case .cancel: continuation.resume(throwing: CancellationError())
                case .wait: break
                }
            }
        } onCancel: {
            let sleeper = lock.withLock { sleepers.removeValue(forKey: id) }
            sleeper?.continuation.resume(throwing: CancellationError())
        }
    }

    /// Moves time forward and resumes every sleeper whose deadline has been reached.
    func advance(by duration: Duration) {
        let due: [Sleeper] = lock.withLock {
            _now = _now.advanced(by: duration)
            let now = _now
            let ready = sleepers.filter { $0.value.deadline <= now }
            for id in ready.keys { sleepers.removeValue(forKey: id) }
            return ready.values.sorted { $0.deadline < $1.deadline }
        }
        for sleeper in due { sleeper.continuation.resume() }
    }
}

/// Waits until `condition` holds, yielding to other tasks between checks.
/// Returns `false` if it still does not hold after roughly two seconds.
func eventually(_ condition: () -> Bool) async -> Bool {
    for _ in 0..<1_000 {
        if condition() { return true }
        await Task.yield()
    }
    for _ in 0..<200 {
        if condition() { return true }
        try? await Task.sleep(for: .milliseconds(10))
    }
    return condition()
}
