import Foundation

/// Owns one flatMapLatest output. Switching, checking a generation and yielding
/// all happen on this actor, with no suspension between validation and yield.
///
/// Output finishes once the outer stream has completed **and** the latest inner
/// stream has completed (or no inner was ever installed). Cancellation finishes immediately.
internal actor _LatestSubscription<T: Sendable> {
    private let continuation: AsyncStream<T>.Continuation
    private var generation: UUID?
    private var current: Task<Void, Never>?
    private var isInnerActive = false
    private var isOuterCompleted = false
    private var isFinished = false

    init(continuation: AsyncStream<T>.Continuation) {
        self.continuation = continuation
    }

    @discardableResult
    func replace(with source: AsyncRay<T>) -> UUID? {
        guard !isFinished else { return nil }
        let next = UUID()
        generation = next
        isInnerActive = true
        current?.cancel()
        current = Task { [weak self] in
            guard !Task.isCancelled else { return }
            for await value in source.stream {
                guard !Task.isCancelled else { return }
                await self?.yield(value, generation: next)
            }
            await self?.innerCompleted(generation: next)
        }
        return next
    }

    func yield(_ value: T, generation expected: UUID) {
        guard !isFinished, generation == expected else { return }
        continuation.yield(value)
    }

    /// Called by an inner producer when its source completes. Stale generations are ignored.
    func innerCompleted(generation expected: UUID) {
        guard !isFinished, generation == expected else { return }
        isInnerActive = false
        if isOuterCompleted { finish() }
    }

    /// Called when the outer stream completes normally. Output finishes after the
    /// latest inner completes, or immediately when no inner is active.
    func outerCompleted() {
        guard !isFinished else { return }
        isOuterCompleted = true
        if !isInnerActive { finish() }
    }

    /// Terminates immediately: cancels the active inner and finishes output.
    func finish() {
        guard !isFinished else { return }
        isFinished = true
        generation = nil
        current?.cancel()
        current = nil
        continuation.finish()
    }

    deinit {
        current?.cancel()
    }
}
