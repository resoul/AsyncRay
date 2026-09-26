import Foundation

/// A one-shot async result container.
///
/// Pending waiters receive the resolved value upon `resolve()`. Resolution is permanent:
/// later calls to `resolve(_:)` are ignored. Subsequent callers receive the stored value.
///
/// ```swift
/// let ready = Once<Bool>()
///
/// // In async producer code:
/// await ready.resolve(true)
///
/// // Await directly (throws CancellationError if the caller is cancelled):
/// let result = try await ready.value
///
/// // Or observe via AsyncRay (emits 1 value and completes):
/// for await value in ready.asyncRay.stream { ... }
/// ```
public actor Once<T: Sendable> {

    private var resolved: T?
    private var legacyWaiters: [CheckedContinuation<T, Never>] = []
    private var cancellableWaiters: [UUID: CheckedContinuation<T?, Never>] = [:]

    /// Creates a pending one-shot result.
    public init() {}

    // MARK: - Resolve

    /// Resolves the value and resumes all current waiters. Subsequent calls are ignored.
    public func resolve(_ value: T) {
        guard resolved == nil else { return }
        resolved = value

        let pendingLegacy = legacyWaiters
        legacyWaiters = []
        pendingLegacy.forEach { $0.resume(returning: value) }

        let pendingCancellable = Array(cancellableWaiters.values)
        cancellableWaiters = [:]
        pendingCancellable.forEach { $0.resume(returning: value) }
    }

    // MARK: - Await

    /// Awaits the resolved value, honouring cancellation of the calling task.
    /// Returns immediately if already resolved.
    ///
    /// - Throws: `CancellationError` if the calling task is cancelled before resolution.
    ///   The waiter is removed, so a cancelled caller never stays suspended or retained.
    ///
    /// ```swift
    /// let token = try await session.token.value
    /// ```
    public var value: T {
        get async throws {
            if let resolved { return resolved }
            try Task.checkCancellation()
            guard let value = await waitCancellable() else { throw CancellationError() }
            return value
        }
    }

    /// Awaits the resolved value. Returns immediately if already resolved.
    ///
    /// - Important: Not cancellation-aware: a cancelled caller stays suspended until
    ///   `resolve(_:)` is called. Prefer `value` when the caller may be cancelled.
    public func wait() async -> T {
        if let value = resolved { return value }
        return await withCheckedContinuation { continuation in
            legacyWaiters.append(continuation)
        }
    }

    /// Internal cancellable await for AsyncRay integration.
    /// Returns `nil` if cancelled before resolution.
    internal func waitCancellable() async -> T? {
        if let value = resolved { return value }
        let id = UUID()
        return await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                cancellableWaiters[id] = continuation
            }
        } onCancel: {
            Task { [weak self] in
                await self?.cancelWaiter(id: id)
            }
        }
    }

    private func cancelWaiter(id: UUID) {
        if let continuation = cancellableWaiters.removeValue(forKey: id) {
            continuation.resume(returning: nil)
        }
    }

    /// The current resolved value, or `nil` if pending (non-blocking).
    ///
    /// When `T` is optional, the result is a nested optional: outer `nil` means pending,
    /// while `.some(nil)` means resolved with a `nil` value.
    public var currentValue: T? { resolved }

    // MARK: - AsyncRay Integration

    /// A single-element `AsyncRay`: awaits resolution, emits the value, and completes.
    /// Cancelling a subscription cancels its wait without affecting other waiters or resolving
    /// the `Once` instance.
    public nonisolated var asyncRay: AsyncRay<T> {
        AsyncRay { [weak self] in
            AsyncStream<T> { continuation in
                let task = Task { [weak self] in
                    guard let self else {
                        continuation.finish()
                        return
                    }
                    if let value = await self.waitCancellable() {
                        guard !Task.isCancelled else { return }
                        continuation.yield(value)
                        continuation.finish()
                    }
                }
                continuation.onTermination = { @Sendable _ in task.cancel() }
            }
        }
    }

    /// Direct `AsyncStream` without `AsyncRay` wrapper.
    public nonisolated var stream: AsyncStream<T> { asyncRay.stream }
}
