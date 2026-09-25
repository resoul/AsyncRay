import Foundation

private final class _StateGate: @unchecked Sendable {
    private let lock = NSLock()
    private var terminated = false
    func terminate() { lock.withLock { terminated = true } }
    var isTerminated: Bool { lock.withLock { terminated } }
}

/// An internal actor that manages a lazy, shared, ref-counted bridge over an `AsyncStream`.
///
/// Pumping starts lazily when the first subscriber connects (`0 -> 1`).
/// When subscriber count drops to zero (`1 -> 0`) or upstream finishes, the pump task is cancelled,
/// the upstream iterator is discarded, and the bridge enters an irreversible terminal state.
/// Subsequent subscribers will immediately receive an empty finished stream.
internal actor SharedAsyncRay<Element: Sendable> {

    private enum State: Sendable {
        case idle(AsyncStream<Element>)
        case active(Task<Void, Never>)
        case terminal
    }

    private var state: State
    private var continuations: [UUID: AsyncStream<Element>.Continuation] = [:]
    private let bufferingPolicy: AsyncStream<Element>.Continuation.BufferingPolicy

    internal init(
        _ stream: AsyncStream<Element>,
        bufferingPolicy: AsyncStream<Element>.Continuation.BufferingPolicy = .bufferingNewest(64)
    ) {
        self.state = .idle(stream)
        self.bufferingPolicy = bufferingPolicy
    }

    deinit {
        if case .active(let task) = state {
            task.cancel()
        }
    }

    /// Count of active subscribers (for testing and diagnostics).
    internal var subscriberCount: Int {
        continuations.count
    }

    /// Registers a new subscriber, starting the upstream pump if transitioning from 0 to 1.
    internal func registerSubscriber(id: UUID, continuation: AsyncStream<Element>.Continuation) {
        if case .terminal = state {
            continuation.finish()
            return
        }

        continuations[id] = continuation

        if case .idle(let stream) = state {
            let task = Task { [weak self] in
                // The pump exclusively owns its iterator; actor state stores only its handle.
                var iterator = stream.makeAsyncIterator()
                while !Task.isCancelled {
                    guard let value = await iterator.next() else { break }
                    guard let self else { break }
                    await self.broadcast(value)
                }
                if let self {
                    await self.finishFromUpstream()
                }
            }
            self.state = .active(task)
        }
    }

    /// Unregisters a subscriber by ID. If active subscribers reach 0, cancels pump and enters terminal state.
    internal func unregisterSubscriber(id: UUID) {
        guard continuations.removeValue(forKey: id) != nil else { return }
        if continuations.isEmpty {
            if case .active(let task) = state {
                task.cancel()
                state = .terminal
            }
        }
    }

    private func broadcast(_ value: Element) {
        guard case .active = state else { return }
        for continuation in continuations.values {
            continuation.yield(value)
        }
    }

    private func finishFromUpstream() {
        guard case .active = state else { return }
        let all = Array(continuations.values)
        continuations = [:]
        state = .terminal
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
                    guard !gate.isTerminated else { return }
                    await self.registerSubscriber(id: id, continuation: continuation)
                }
            }
        }
    }
}
