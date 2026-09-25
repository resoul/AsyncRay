// Shared test helpers

import Foundation

/// Thread-safe value collector for tests.
/// Synchronously appends values under lock to preserve deterministic execution order without spawning unmanaged tasks.
final class Collector<T: Sendable>: @unchecked Sendable {
    private let lock = NSLock()
    private var _values: [T] = []

    var values: [T] {
        lock.withLock { _values }
    }

    func append(_ v: T) {
        lock.withLock { _values.append(v) }
    }

    func count() -> Int {
        lock.withLock { _values.count }
    }

    func reset() {
        lock.withLock { _values = [] }
    }
}
