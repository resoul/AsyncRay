extension AsyncThrowingRay {

    // MARK: - Transformation

    /// Synchronously transforms each value. Upstream failure is forwarded unchanged.
    public func map<U: Sendable>(
        _ transform: @Sendable @escaping (T) -> U
    ) -> AsyncThrowingRay<U> {
        chainedTask { stream, continuation in
            for try await value in stream {
                continuation.yield(transform(value))
            }
        }
    }

    /// Transforms each value with a throwing closure. A thrown error fails the stream and
    /// cancels upstream.
    ///
    /// ```swift
    /// frames.tryMap { try JSONDecoder().decode(Event.self, from: $0) }
    /// ```
    public func tryMap<U: Sendable>(
        _ transform: @Sendable @escaping (T) throws -> U
    ) -> AsyncThrowingRay<U> {
        chainedTask { stream, continuation in
            for try await value in stream {
                continuation.yield(try transform(value))
            }
        }
    }

    /// Asynchronously transforms each value with a throwing closure, one value at a time.
    /// A thrown error fails the stream and cancels upstream.
    ///
    /// ```swift
    /// ids.asyncMap { id in try await api.fetchUser(id) }
    /// ```
    public func asyncMap<U: Sendable>(
        _ transform: @Sendable @escaping (T) async throws -> U
    ) -> AsyncThrowingRay<U> {
        chainedTask { stream, continuation in
            for try await value in stream {
                guard !Task.isCancelled else { return }
                continuation.yield(try await transform(value))
            }
        }
    }

    /// Transforms each value and forwards only non-nil results.
    public func compactMap<U: Sendable>(
        _ transform: @Sendable @escaping (T) -> U?
    ) -> AsyncThrowingRay<U> {
        chainedTask { stream, continuation in
            for try await value in stream {
                if let mapped = transform(value) { continuation.yield(mapped) }
            }
        }
    }

    /// Forwards only values matching the predicate.
    public func filter(
        _ predicate: @Sendable @escaping (T) -> Bool
    ) -> AsyncThrowingRay<T> {
        chainedTask { stream, continuation in
            for try await value in stream {
                if predicate(value) { continuation.yield(value) }
            }
        }
    }

    /// Runs a side effect when the stream fails, then forwards the failure unchanged.
    /// Useful for logging. The handler does not run on completion or cancellation.
    public func onFailure(
        _ handler: @Sendable @escaping (Error) -> Void
    ) -> AsyncThrowingRay<T> {
        chainedTask { stream, continuation in
            do {
                for try await value in stream {
                    continuation.yield(value)
                }
            } catch {
                if !Task.isCancelled { handler(error) }
                throw error
            }
        }
    }

    // MARK: - Error Handling

    /// Replaces a failure with the values of a fallback stream and returns a non-failing stream.
    ///
    /// Values emitted before the failure are kept. The fallback is subscribed only when upstream
    /// fails; the output completes when upstream completes or the fallback completes.
    ///
    /// ```swift
    /// api.fetchFeed()
    ///     .catch { _ in AsyncRay.just(cachedFeed) }
    ///     .sinkOnMain { feedView.show($0) }
    /// ```
    public func `catch`(
        _ handler: @Sendable @escaping (Error) -> AsyncRay<T>
    ) -> AsyncRay<T> {
        chainedTaskToAsyncRay { stream, continuation in
            do {
                for try await value in stream {
                    continuation.yield(value)
                }
            } catch {
                guard !Task.isCancelled else { return }
                for await value in handler(error)._make() {
                    continuation.yield(value)
                }
            }
        }
    }

    /// Replaces a failure with a single value and completes, returning a non-failing stream.
    public func replaceError(with value: T) -> AsyncRay<T> {
        self.catch { _ in .just(value) }
    }

    /// Materializes values and the failure as `Result`s in a non-failing stream.
    ///
    /// Each value becomes `.success`; a failure becomes one final `.failure` before completion.
    public func asResult() -> AsyncRay<Result<T, Error>> {
        chainedTaskToAsyncRay { stream, continuation in
            do {
                for try await value in stream {
                    continuation.yield(.success(value))
                }
            } catch {
                guard !Task.isCancelled else { return }
                continuation.yield(.failure(error))
            }
        }
    }

    /// Resubscribes to upstream after a failure, up to `maxRetries` times.
    ///
    /// Each retry runs the cold producer again. Values emitted by failed attempts are already
    /// delivered and are not retracted. After the last retry fails, the output fails with that
    /// error.
    public func retry(_ maxRetries: Int) -> AsyncThrowingRay<T> {
        retry(maxRetries, delay: .zero, clock: ContinuousClock())
    }

    /// Resubscribes to upstream after a failure, up to `maxRetries` times, waiting `delay`
    /// before each retry.
    ///
    /// ```swift
    /// AsyncThrowingRay.from { try await api.fetchConfig() }
    ///     .retry(3, delay: .seconds(2))
    /// ```
    public func retry(_ maxRetries: Int, delay: Duration) -> AsyncThrowingRay<T> {
        retry(maxRetries, delay: delay, clock: ContinuousClock())
    }

    /// Resubscribes to upstream after a failure, up to `maxRetries` times, waiting `delay`
    /// measured by `clock` before each retry. Cancellation during the wait ends the stream
    /// without reporting the failure.
    public func retry<C: Clock>(_ maxRetries: Int, delay: Duration, clock: C) -> AsyncThrowingRay<T>
    where C.Duration == Duration {
        precondition(maxRetries >= 0, "retry requires maxRetries >= 0")
        let policy = inheritedBufferingPolicy
        return AsyncThrowingRay(inheriting: policy) { [_make] in
            makeAsyncThrowingRayStream(policy: policy) { continuation in
                let task = Task {
                    var attempt = 0
                    while true {
                        do {
                            for try await value in _make() {
                                continuation.yield(value)
                            }
                            guard !Task.isCancelled else { return }
                            continuation.finish()
                            return
                        } catch {
                            guard !Task.isCancelled else { return }
                            guard attempt < maxRetries else {
                                continuation.finish(throwing: error)
                                return
                            }
                            attempt += 1
                        }
                        if delay > .zero {
                            do {
                                try await clock.sleep(for: delay)
                            } catch {
                                return  // Cancelled while waiting to retry
                            }
                        }
                    }
                }
                continuation.onTermination = { @Sendable _ in task.cancel() }
            }
        }
    }
}

// MARK: - AsyncRay → AsyncThrowingRay

extension AsyncRay {

    /// Views this stream as an `AsyncThrowingRay` that never fails, so it can be combined with
    /// failing operators.
    public func asThrowing() -> AsyncThrowingRay<T> {
        let policy = inheritedBufferingPolicy
        return AsyncThrowingRay(inheriting: policy) { [_make] in
            makeAsyncThrowingRayStream(policy: policy) { continuation in
                let task = Task {
                    for await value in _make() {
                        continuation.yield(value)
                    }
                    guard !Task.isCancelled else { return }
                    continuation.finish()
                }
                continuation.onTermination = { @Sendable _ in task.cancel() }
            }
        }
    }

    /// Transforms each value with a throwing closure, returning a stream that fails with the
    /// first thrown error.
    ///
    /// ```swift
    /// socketFrames.asyncRay.tryMap { try JSONDecoder().decode(Message.self, from: $0) }
    /// ```
    public func tryMap<U: Sendable>(
        _ transform: @Sendable @escaping (T) throws -> U
    ) -> AsyncThrowingRay<U> {
        asThrowing().tryMap(transform)
    }
}
