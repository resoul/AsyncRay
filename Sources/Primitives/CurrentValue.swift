// Observable state holder with asynchronous replay of the current value.

import Foundation

/// Synchronous storage owned exclusively by one CurrentValue actor.
/// No reference to this non-Sendable object crosses its owner's isolation boundary.
internal final class _CurrentValueStorage<T: Sendable> {
    private(set) var value: T
    private var continuations: [UUID: AsyncStream<T>.Continuation] = [:]

    init(_ initial: T) {
        value = initial
    }

    var subscriberCount: Int { continuations.count }

    func set(_ newValue: T) {
        value = newValue
        for continuation in continuations.values {
            continuation.yield(newValue)
        }
    }

    func modify(_ transform: (T) -> T) {
        set(transform(value))
    }

    func setDistinct(_ newValue: T) where T: Equatable {
        guard newValue != value else { return }
        set(newValue)
    }

    func modifyDistinct(_ transform: (T) -> T) where T: Equatable {
        setDistinct(transform(value))
    }

    func registerAndReplay(id: UUID, continuation: AsyncStream<T>.Continuation) {
        continuations[id] = continuation
        // Registration and replay execute in one owner-actor turn. If cancellation
        // beat registration, yield detects the terminated stream and removes it.
        if case .terminated = continuation.yield(value) {
            continuations.removeValue(forKey: id)
        }
    }

    func removeContinuation(id: UUID) {
        continuations.removeValue(forKey: id)
    }

    deinit {
        for continuation in continuations.values { continuation.finish() }
    }
}

private func currentValueAsyncRay<T: Sendable>(
    register: @escaping @Sendable (UUID, AsyncStream<T>.Continuation) async -> Void,
    remove: @escaping @Sendable (UUID) async -> Void
) -> AsyncRay<T> {
    AsyncRay(bufferingPolicy: .bufferingNewest(1)) {
        AsyncStream<T>(bufferingPolicy: .bufferingNewest(1)) { continuation in
            let id = UUID()
            continuation.onTermination = { @Sendable _ in
                Task { await remove(id) }
            }
            Task { await register(id, continuation) }
        }
    }
}

/// An observable state holder that retains the latest value and notifies subscribers upon change.
///
/// **Replay behavior:** A new subscriber asynchronously receives the current value (replay = 1),
/// making it suitable for states (e.g. connection state, auth status).
///
/// ```swift
/// let state = CurrentValue(ConnectionState.disconnected)
///
/// // UI subscription — immediately receives .disconnected, then all updates
/// state.asyncRay.sinkOnMain { state in label.stringValue = state.description }
///     .store(in: bag)
///
/// // Update:
/// await state.set(.connected)
/// await state.modify { _ in .reconnecting(attempt: 1) }
/// ```
public actor CurrentValue<T: Sendable> {

    private let storage: _CurrentValueStorage<T>

    public init(_ initial: T) {
        self.storage = _CurrentValueStorage(initial)
    }

    // MARK: - Read

    /// Reads the current value on this actor.
    public var value: T {
        get async { storage.value }
    }

    // MARK: - Write

    /// Sets a new value and notifies all active subscribers.
    public func set(_ newValue: T) async {
        storage.set(newValue)
    }

    /// Atomically transforms and publishes the value on this actor, without suspension.
    public func modify(_ transform: (T) -> T) async {
        storage.modify(transform)
    }

    // MARK: - Subscription

    /// Observes current state asynchronously, then the latest updates (buffer size 1).
    public nonisolated var asyncRay: AsyncRay<T> {
        currentValueAsyncRay(
            register: { [weak self] id, continuation in
                guard let self else { continuation.finish(); return }
                await self.register(id: id, continuation: continuation)
            },
            remove: { [weak self] id in
                await self?.remove(id: id)
            }
        )
    }

    /// Direct `AsyncStream` without `AsyncRay` wrapper.
    public nonisolated var stream: AsyncStream<T> { asyncRay.stream }

    private func register(id: UUID, continuation: AsyncStream<T>.Continuation) {
        storage.registerAndReplay(id: id, continuation: continuation)
    }

    private func remove(id: UUID) {
        storage.removeContinuation(id: id)
    }
}

// MARK: - CurrentValueDistinct (Built-in deduplication)

/// A state holder actor similar to `CurrentValue`, but skips notifying subscribers when the new value equals the current value.
public actor CurrentValueDistinct<T: Sendable & Equatable> {

    private let storage: _CurrentValueStorage<T>

    public init(_ initial: T) {
        self.storage = _CurrentValueStorage(initial)
    }

    public var value: T {
        get async { storage.value }
    }

    /// Sets a new value. If identical to the current value, subscribers are not notified.
    public func set(_ newValue: T) async {
        storage.setDistinct(newValue)
    }

    public func modify(_ transform: (T) -> T) async {
        storage.modifyDistinct(transform)
    }

    public nonisolated var asyncRay: AsyncRay<T> {
        currentValueAsyncRay(
            register: { [weak self] id, continuation in
                guard let self else { continuation.finish(); return }
                await self.register(id: id, continuation: continuation)
            },
            remove: { [weak self] id in
                await self?.remove(id: id)
            }
        )
    }

    public nonisolated var stream: AsyncStream<T> { asyncRay.stream }

    private func register(id: UUID, continuation: AsyncStream<T>.Continuation) {
        storage.registerAndReplay(id: id, continuation: continuation)
    }

    private func remove(id: UUID) {
        storage.removeContinuation(id: id)
    }
}
