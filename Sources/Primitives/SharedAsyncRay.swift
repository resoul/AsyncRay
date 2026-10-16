import Foundation

private final class _StateGate: @unchecked Sendable {
    private let lock = NSLock()
    private var terminated = false
    func terminate() { lock.withLock { terminated = true } }
    var isTerminated: Bool { lock.withLock { terminated } }
}

/// An internal actor that manages a lazy, shared, ref-counted connection to an upstream stream.
///
/// Pumping starts lazily when the first subscriber connects (`0 -> 1`).
/// When subscriber count drops to zero (`1 -> 0`) or upstream finishes, the pump task is cancelled
/// and the upstream iterator is discarded. What happens next depends on the source:
/// - A one-shot source (an existing `AsyncStream`) enters an irreversible terminal state, and
///   subsequent subscribers immediately receive an empty finished stream.
/// - A restartable source (a cold `AsyncRay` factory) returns to idle, and the next subscriber
///   starts a fresh upstream subscription.
internal actor SharedAsyncRay<Element: Sendable> {

    private enum State: Sendable {
        case idle
        case active(Task<Void, Never>, connection: Int)
        case terminal
    }

    private var state: State = .idle
    private var connection = 0
    private var continuations: [UUID: AsyncStream<Element>.Continuation] = [:]
    private let makeUpstream: @Sendable () -> AsyncStream<Element>
    private let isRestartable: Bool
    private let bufferingPolicy: AsyncStream<Element>.Continuation.BufferingPolicy

    /// Shares a single-consumer `AsyncStream`. The bridge is one-shot.
    internal init(
        _ stream: AsyncStream<Element>,
        bufferingPolicy: AsyncStream<Element>.Continuation.BufferingPolicy = .bufferingNewest(64)
    ) {
        self.makeUpstream = { stream }
        self.isRestartable = false
        self.bufferingPolicy = bufferingPolicy
    }

    /// Shares a cold source. Each connection calls `make` for a fresh upstream subscription.
    internal init(
        restartable make: @Sendable @escaping () -> AsyncStream<Element>,
        bufferingPolicy: AsyncStream<Element>.Continuation.BufferingPolicy
    ) {
        self.makeUpstream = make
        self.isRestartable = true
        self.bufferingPolicy = bufferingPolicy
    }

    deinit {
        if case .active(let task, _) = state {
            task.cancel()
        }
    }

    /// Count of active subscribers (for testing and diagnostics).
    internal var subscriberCount: Int {
        continuations.count
    }

    /// Registers a new subscriber, starting the upstream pump if transitioning from 0 to 1.
    ///
    /// `isCancelled` is checked on the actor: a subscriber whose stream terminated before this
    /// call is skipped, and one that terminates later is removed by its queued unregister, which
    /// the actor runs after this call. Either way no stale continuation keeps the pump alive.
    internal func registerSubscriber(
        id: UUID,
        continuation: AsyncStream<Element>.Continuation,
        isCancelled: @Sendable () -> Bool = { false }
    ) {
        guard !isCancelled() else { return }
        if case .terminal = state {
            continuation.finish()
            return
        }

        continuations[id] = continuation

        if case .idle = state {
            connection += 1
            let current = connection
            let stream = makeUpstream()
            let task = Task { [weak self] in
                // The pump exclusively owns its iterator; actor state stores only its handle.
                var iterator = stream.makeAsyncIterator()
                while !Task.isCancelled {
                    guard let value = await iterator.next() else { break }
                    guard let self else { break }
                    await self.broadcast(value, connection: current)
                }
                if let self {
                    await self.finishFromUpstream(connection: current)
                }
            }
            self.state = .active(task, connection: current)
        }
    }

    /// Unregisters a subscriber by ID. If active subscribers reach 0, cancels the pump and
    /// returns to idle (restartable) or enters the terminal state (one-shot).
    internal func unregisterSubscriber(id: UUID) {
        guard continuations.removeValue(forKey: id) != nil else { return }
        if continuations.isEmpty {
            if case .active(let task, _) = state {
                task.cancel()
                state = isRestartable ? .idle : .terminal
            }
        }
    }

    // A cancelled pump may still be draining after a restart; the connection number keeps it
    // from delivering to, or finishing, subscribers of the newer connection.
    private func isCurrent(_ expected: Int) -> Bool {
        guard case .active(_, let current) = state else { return false }
        return current == expected
    }

    private func broadcast(_ value: Element, connection expected: Int) {
        guard isCurrent(expected) else { return }
        for continuation in continuations.values {
            continuation.yield(value)
        }
    }

    private func finishFromUpstream(connection expected: Int) {
        guard isCurrent(expected) else { return }
        let all = Array(continuations.values)
        continuations = [:]
        state = isRestartable ? .idle : .terminal
        all.forEach { $0.finish() }
    }

    /// Exposes a `AsyncRay` wrapper around this shared stream.
    internal nonisolated var asyncRay: AsyncRay<Element> {
        let policy = bufferingPolicy
        return AsyncRay(bufferingPolicy: policy) { [self] in
            AsyncStream<Element>(bufferingPolicy: policy) { continuation in
                let id = UUID()
                let gate = _StateGate()

                continuation.onTermination = { @Sendable [self] _ in
                    gate.terminate()
                    Task {
                        await self.unregisterSubscriber(id: id)
                    }
                }

                Task {
                    await self.registerSubscriber(
                        id: id,
                        continuation: continuation,
                        isCancelled: { gate.isTerminated }
                    )
                }
            }
        }
    }
}
