import Foundation

// MARK: - Internal helpers

// Each producer task owns its iterator for its entire lifetime. The actor coordinates
// at most one pending value per side, never stores or transfers an AsyncIterator.
private actor _ZipRendezvous<A: Sendable, B: Sendable> {
    private var pendingA: (A, AsyncStream<Void>.Continuation)?
    private var pendingB: (B, AsyncStream<Void>.Continuation)?
    private var isFinished = false
    private let output: AsyncStream<(A, B)>.Continuation

    init(_ output: AsyncStream<(A, B)>.Continuation) {
        self.output = output
    }

    func offerA(_ value: A) async {
        guard !isFinished, !Task.isCancelled else { return }
        let acknowledgment = AsyncStream<Void>(bufferingPolicy: .bufferingNewest(1)) {
            continuation in
            pendingA = (value, continuation)
        }
        emitPairIfReady()
        // AsyncStream waiting is cancellation-aware, unlike an unguarded continuation.
        for await _ in acknowledgment {}
    }

    func offerB(_ value: B) async {
        guard !isFinished, !Task.isCancelled else { return }
        let acknowledgment = AsyncStream<Void>(bufferingPolicy: .bufferingNewest(1)) {
            continuation in
            pendingB = (value, continuation)
        }
        emitPairIfReady()
        for await _ in acknowledgment {}
    }

    private func emitPairIfReady() {
        guard let a = pendingA, let b = pendingB else { return }
        pendingA = nil
        pendingB = nil
        output.yield((a.0, b.0))
        a.1.finish()
        b.1.finish()
    }

    func finish() {
        guard !isFinished else { return }
        isFinished = true
        pendingA?.1.finish()
        pendingB?.1.finish()
        pendingA = nil
        pendingB = nil
    }
}

/// Thread-safe latest-values container for combineLatest.
///
/// The combined tuple is built **and yielded** under the same lock, so the output order
/// always matches the order of state updates and the last emitted tuple is the latest state.
/// `yield` never blocks or runs user code, so calling it under the lock is safe.
internal final class _CombineLatestState2<A: Sendable, B: Sendable>: @unchecked Sendable {
    private let lock = NSLock()
    private let output: AsyncStream<(A, B)>.Continuation
    private var a: A?
    private var b: B?

    init(_ output: AsyncStream<(A, B)>.Continuation) {
        self.output = output
    }

    func setA(_ v: A) {
        lock.withLock {
            a = v
            guard let b else { return }
            output.yield((v, b))
        }
    }

    func setB(_ v: B) {
        lock.withLock {
            b = v
            guard let a else { return }
            output.yield((a, v))
        }
    }
}

/// Direct 3-stream combination state, avoiding intermediate nested stream allocations.
internal final class _CombineLatestState3<A: Sendable, B: Sendable, C: Sendable>:
    @unchecked Sendable
{
    private let lock = NSLock()
    private let output: AsyncStream<(A, B, C)>.Continuation
    private var a: A?
    private var b: B?
    private var c: C?

    init(_ output: AsyncStream<(A, B, C)>.Continuation) {
        self.output = output
    }

    func setA(_ v: A) {
        lock.withLock {
            a = v
            guard let b, let c else { return }
            output.yield((v, b, c))
        }
    }

    func setB(_ v: B) {
        lock.withLock {
            b = v
            guard let a, let c else { return }
            output.yield((a, v, c))
        }
    }

    func setC(_ v: C) {
        lock.withLock {
            c = v
            guard let a, let b else { return }
            output.yield((a, b, v))
        }
    }
}

/// Direct 4-stream combination state.
internal final class _CombineLatestState4<A: Sendable, B: Sendable, C: Sendable, D: Sendable>:
    @unchecked Sendable
{
    private let lock = NSLock()
    private let output: AsyncStream<(A, B, C, D)>.Continuation
    private var a: A?
    private var b: B?
    private var c: C?
    private var d: D?

    init(_ output: AsyncStream<(A, B, C, D)>.Continuation) {
        self.output = output
    }

    func setA(_ v: A) {
        lock.withLock {
            a = v
            guard let b, let c, let d else { return }
            output.yield((v, b, c, d))
        }
    }

    func setB(_ v: B) {
        lock.withLock {
            b = v
            guard let a, let c, let d else { return }
            output.yield((a, v, c, d))
        }
    }

    func setC(_ v: C) {
        lock.withLock {
            c = v
            guard let a, let b, let d else { return }
            output.yield((a, b, v, d))
        }
    }

    func setD(_ v: D) {
        lock.withLock {
            d = v
            guard let a, let b, let c else { return }
            output.yield((a, b, c, v))
        }
    }
}

// MARK: - merge

/// Merges multiple streams of the same element type into one, with an explicit output buffering policy.
///
/// Values from all streams are forwarded as they arrive. Order between distinct sources is not guaranteed.
/// Cancelling the output subscription cancels every input subscription. The output buffering
/// policy controls how values are retained when downstream is slower than the combined inputs.
///
/// ```swift
/// let updates = merge([localChanges, serverPush, notifications], bufferingPolicy: .bufferingNewest(64))
/// ```
public func merge<T: Sendable>(
    _ asyncRays: [AsyncRay<T>],
    bufferingPolicy: AsyncStream<T>.Continuation.BufferingPolicy
) -> AsyncRay<T> {
    _mergeArray(asyncRays, bufferingPolicy: bufferingPolicy)
}

private func _mergeArray<T: Sendable>(
    _ asyncRays: [AsyncRay<T>],
    bufferingPolicy: AsyncStream<T>.Continuation.BufferingPolicy
) -> AsyncRay<T> {
    guard !asyncRays.isEmpty else { return .empty() }
    return AsyncRay<T>(bufferingPolicy: bufferingPolicy) {
        AsyncStream<T>(bufferingPolicy: bufferingPolicy) { continuation in
            let task = Task {
                await withTaskGroup(of: Void.self) { group in
                    for asyncRay in asyncRays {
                        group.addTask {
                            for await value in asyncRay._make() {
                                _ = continuation.yield(value)
                            }
                        }
                    }
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }
}

/// Deprecated unbounded overload retained for source compatibility.
/// Merges the streams using an unbounded output buffer.
///
/// - Important: Prefer `merge(_:bufferingPolicy:)` and choose an explicit buffer limit.
@available(
    *,
    deprecated,
    message:
        "Use merge(_:bufferingPolicy:) — an unbounded merge output stream can grow without limit under a slow downstream consumer."
)
public func merge<T: Sendable>(_ asyncRays: AsyncRay<T>...) -> AsyncRay<T> {
    _mergeArray(asyncRays, bufferingPolicy: .unbounded)
}

/// Merges the streams using an unbounded output buffer.
///
/// - Important: Prefer `merge(_:bufferingPolicy:)` and choose an explicit buffer limit.
@available(
    *,
    deprecated,
    message:
        "Use merge(_:bufferingPolicy:) — an unbounded merge output stream can grow without limit under a slow downstream consumer."
)
public func merge<T: Sendable>(_ asyncRays: [AsyncRay<T>]) -> AsyncRay<T> {
    _mergeArray(asyncRays, bufferingPolicy: .unbounded)
}

// MARK: - combineLatest

/// Combines the latest values of two streams into a tuple with an explicit output buffering policy.
/// Emits whenever either stream produces a value, once both have emitted at least once.
///
/// **Completion semantics:** The combined stream finishes only when **all** input streams have finished.
/// If one input stream completes while another continues emitting, new emissions will continue to be paired
/// with the last value of the completed stream.
/// The output `bufferingPolicy` applies to combined tuples; a bounded policy can drop older or newer tuples
/// according to the selected `AsyncStream` policy.
///
/// ```swift
/// combineLatest(username.asyncRay, password.asyncRay, bufferingPolicy: .bufferingNewest(1))
///     .map { user, pass in !user.isEmpty && pass.count >= 8 }
///     .sinkOnMain { loginButton.isEnabled = $0 }
/// ```
public func combineLatest<A: Sendable, B: Sendable>(
    _ fa: AsyncRay<A>,
    _ fb: AsyncRay<B>,
    bufferingPolicy: AsyncStream<(A, B)>.Continuation.BufferingPolicy
) -> AsyncRay<(A, B)> {
    AsyncRay(bufferingPolicy: bufferingPolicy) {
        AsyncStream<(A, B)>(bufferingPolicy: bufferingPolicy) { continuation in
            let state = _CombineLatestState2<A, B>(continuation)
            let task = Task {
                await withTaskGroup(of: Void.self) { group in
                    group.addTask {
                        for await v in fa._make() {
                            state.setA(v)
                        }
                    }
                    group.addTask {
                        for await v in fb._make() {
                            state.setB(v)
                        }
                    }
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }
}

/// Combines the latest values of three streams into a tuple with an explicit output buffering policy.
///
/// **Completion semantics:** Finishes only after all 3 streams finish. Completed streams retain their last value.
public func combineLatest<A: Sendable, B: Sendable, C: Sendable>(
    _ fa: AsyncRay<A>,
    _ fb: AsyncRay<B>,
    _ fc: AsyncRay<C>,
    bufferingPolicy: AsyncStream<(A, B, C)>.Continuation.BufferingPolicy
) -> AsyncRay<(A, B, C)> {
    AsyncRay(bufferingPolicy: bufferingPolicy) {
        AsyncStream<(A, B, C)>(bufferingPolicy: bufferingPolicy) { continuation in
            let state = _CombineLatestState3<A, B, C>(continuation)
            let task = Task {
                await withTaskGroup(of: Void.self) { group in
                    group.addTask {
                        for await v in fa._make() {
                            state.setA(v)
                        }
                    }
                    group.addTask {
                        for await v in fb._make() {
                            state.setB(v)
                        }
                    }
                    group.addTask {
                        for await v in fc._make() {
                            state.setC(v)
                        }
                    }
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }
}

/// Combines the latest values of four streams into a tuple with an explicit output buffering policy.
///
/// **Completion semantics:** Finishes only after all 4 streams finish. Completed streams retain their last value.
public func combineLatest<A: Sendable, B: Sendable, C: Sendable, D: Sendable>(
    _ fa: AsyncRay<A>,
    _ fb: AsyncRay<B>,
    _ fc: AsyncRay<C>,
    _ fd: AsyncRay<D>,
    bufferingPolicy: AsyncStream<(A, B, C, D)>.Continuation.BufferingPolicy
) -> AsyncRay<(A, B, C, D)> {
    AsyncRay(bufferingPolicy: bufferingPolicy) {
        AsyncStream<(A, B, C, D)>(bufferingPolicy: bufferingPolicy) { continuation in
            let state = _CombineLatestState4<A, B, C, D>(continuation)
            let task = Task {
                await withTaskGroup(of: Void.self) { group in
                    group.addTask {
                        for await v in fa._make() {
                            state.setA(v)
                        }
                    }
                    group.addTask {
                        for await v in fb._make() {
                            state.setB(v)
                        }
                    }
                    group.addTask {
                        for await v in fc._make() {
                            state.setC(v)
                        }
                    }
                    group.addTask {
                        for await v in fd._make() {
                            state.setD(v)
                        }
                    }
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }
}

/// Combines the latest values of two streams using an unbounded output buffer.
///
/// - Important: Prefer `combineLatest(_:_:bufferingPolicy:)` and choose an explicit buffer limit.
@available(
    *,
    deprecated,
    message:
        "Use combineLatest(_:_:bufferingPolicy:) — an unbounded output stream can grow without limit under a slow downstream consumer."
)
public func combineLatest<A: Sendable, B: Sendable>(
    _ fa: AsyncRay<A>,
    _ fb: AsyncRay<B>
) -> AsyncRay<(A, B)> {
    combineLatest(fa, fb, bufferingPolicy: .unbounded)
}

/// Combines the latest values of three streams using an unbounded output buffer.
///
/// - Important: Prefer `combineLatest(_:_:_:bufferingPolicy:)` and choose an explicit buffer limit.
@available(
    *,
    deprecated,
    message:
        "Use combineLatest(_:_:_:bufferingPolicy:) — an unbounded output stream can grow without limit under a slow downstream consumer."
)
public func combineLatest<A: Sendable, B: Sendable, C: Sendable>(
    _ fa: AsyncRay<A>,
    _ fb: AsyncRay<B>,
    _ fc: AsyncRay<C>
) -> AsyncRay<(A, B, C)> {
    combineLatest(fa, fb, fc, bufferingPolicy: .unbounded)
}

/// Combines the latest values of four streams using an unbounded output buffer.
///
/// - Important: Prefer `combineLatest(_:_:_:_:bufferingPolicy:)` and choose an explicit buffer limit.
@available(
    *,
    deprecated,
    message:
        "Use combineLatest(_:_:_:_:bufferingPolicy:) — an unbounded output stream can grow without limit under a slow downstream consumer."
)
public func combineLatest<A: Sendable, B: Sendable, C: Sendable, D: Sendable>(
    _ fa: AsyncRay<A>,
    _ fb: AsyncRay<B>,
    _ fc: AsyncRay<C>,
    _ fd: AsyncRay<D>
) -> AsyncRay<(A, B, C, D)> {
    combineLatest(fa, fb, fc, fd, bufferingPolicy: .unbounded)
}

// MARK: - zip

/// Pairs values from two streams 1-to-1 with an explicit output buffering policy.
///
/// Both input streams are read concurrently and values are paired in arrival order, one from each input.
/// When either input completes, any unmatched value is discarded, the remaining input is cancelled, and
/// the output finishes. The output `bufferingPolicy` controls retention when downstream is slower.
public func zip<A: Sendable, B: Sendable>(
    _ fa: AsyncRay<A>,
    _ fb: AsyncRay<B>,
    bufferingPolicy: AsyncStream<(A, B)>.Continuation.BufferingPolicy
) -> AsyncRay<(A, B)> {
    AsyncRay<(A, B)>(bufferingPolicy: bufferingPolicy) {
        AsyncStream<(A, B)>(bufferingPolicy: bufferingPolicy) { continuation in
            let task = Task {
                let rendezvous = _ZipRendezvous<A, B>(continuation)
                await withTaskGroup(of: Void.self) { group in
                    group.addTask {
                        for await value in fa._make() {
                            guard !Task.isCancelled else { break }
                            await rendezvous.offerA(value)
                        }
                    }
                    group.addTask {
                        for await value in fb._make() {
                            guard !Task.isCancelled else { break }
                            await rendezvous.offerB(value)
                        }
                    }
                    // Whichever side ends first releases any unmatched value and the
                    // other pending read. No left-first await that can mask right EOF.
                    await group.next()
                    await rendezvous.finish()
                    group.cancelAll()
                }
                guard !Task.isCancelled else { return }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }
}

/// Deprecated unbounded overload retained for source compatibility.
@available(
    *,
    deprecated,
    message:
        "Use zip(_:_:bufferingPolicy:) — an unbounded zip output stream can grow without limit under a slow downstream consumer."
)
public func zip<A: Sendable, B: Sendable>(
    _ fa: AsyncRay<A>,
    _ fb: AsyncRay<B>
) -> AsyncRay<(A, B)> {
    zip(fa, fb, bufferingPolicy: .unbounded)
}

// MARK: - AsyncRay extension: merge

extension AsyncRay {
    /// Merges this stream with another of the same element type.
    public func merge(
        with other: AsyncRay<T>,
        bufferingPolicy: AsyncStream<T>.Continuation.BufferingPolicy
    ) -> AsyncRay<T> {
        _mergeArray([self, other], bufferingPolicy: bufferingPolicy)
    }

    /// Merges this stream with another using an unbounded output buffer.
    ///
    /// - Important: Prefer `merge(with:bufferingPolicy:)` and choose an explicit buffer limit.
    @available(
        *,
        deprecated,
        message:
            "Use merge(with:bufferingPolicy:) — an unbounded merge output stream can grow without limit under a slow downstream consumer."
    )
    public func merge(with other: AsyncRay<T>) -> AsyncRay<T> {
        _mergeArray([self, other], bufferingPolicy: .unbounded)
    }
}
