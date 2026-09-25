// Thread-safe container for active subscriptions

import Foundation

/// A subscription container that cancels all retained subscriptions upon `cancelAll()` or `deinit`.
/// Completed subscriptions are automatically pruned from the bag to prevent unbounded memory growth.
///
/// Typical usage in a ViewController / ViewModel:
/// ```swift
/// private let bag = SubscriptionBag()
///
/// func viewDidLoad() {
///     someAsyncRay.sinkOnMain { value in ... }
///         .store(in: bag)
/// }
/// // When the ViewController deinits, all subscriptions are automatically cancelled
/// ```
public final class SubscriptionBag: @unchecked Sendable {

    private let lock = NSLock()
    private var subscriptions: [UUID: Subscription] = [:]

    public init() {}

    /// Count of active subscriptions retained in the bag.
    public var count: Int {
        lock.withLock { subscriptions.count }
    }

    /// Adds a subscription to the bag.
    public func add(_ subscription: Subscription) {
        let id = subscription.id
        lock.withLock {
            subscriptions[id] = subscription
        }
        subscription.addObserver { [weak self] in
            guard let self else { return }
            self.lock.withLock {
                _ = self.subscriptions.removeValue(forKey: id)
            }
        }
    }

    /// Cancels all subscriptions and clears the bag.
    public func cancelAll() {
        let all = lock.withLock { () -> [Subscription] in
            let copy = Array(subscriptions.values)
            subscriptions = [:]
            return copy
        }
        all.forEach { $0.cancel() }
    }

    deinit {
        cancelAll()
    }
}
