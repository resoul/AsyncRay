extension AsyncRay {

    // MARK: - onMain

    /// Dispatches value delivery onto `MainActor`.
    ///
    /// Useful for updating UI from background streams.
    ///
    /// ```swift
    /// networkAsyncRay
    ///     .map { parseResponse($0) }
    ///     .onMain()
    ///     .sink { tableView.reload($0) }
    /// ```
    ///
    /// - Note: For UI subscriptions, prefer `sinkOnMain` directly as it avoids an extra intermediate stream.
    public func onMain() -> AsyncRay<T> {
        chained { continuation, stream in
            let task = Task { @MainActor in
                for await value in stream {
                    continuation.yield(value)
                }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }

    // MARK: - onBackground

    /// Iterates the upstream stream in a detached task with the given priority and forwards
    /// values from that task. No actor context or task-local values are inherited by the detached task.
    /// Cancelling the downstream stream cancels the forwarding task.
    public func onBackground(priority: TaskPriority = .background) -> AsyncRay<T> {
        chained { continuation, stream in
            let task = Task.detached(priority: priority) {
                for await value in stream {
                    continuation.yield(value)
                }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }

    // MARK: - handleEvents

    /// Runs a synchronous side effect for each value before forwarding that value unchanged.
    /// Useful for debugging, logging, or metrics. The handler runs on the operator's task and
    /// should not block for long periods.
    /// ```swift
    /// asyncRay.handleEvents { print("Got: \($0)") }.sink { process($0) }
    /// ```
    public func handleEvents(
        _ handler: @Sendable @escaping (T) -> Void
    ) -> AsyncRay<T> {
        map { value in
            handler(value)
            return value
        }
    }

    /// Executes a side effect upon completion of the stream.
    /// Does not execute the handler if the subscription was cancelled.
    /// The handler runs after the upstream ends normally and before the downstream finishes.
    public func onCompletion(
        _ handler: @Sendable @escaping () -> Void
    ) -> AsyncRay<T> {
        chained { continuation, stream in
            let task = Task {
                for await value in stream {
                    continuation.yield(value)
                }
                guard !Task.isCancelled else { return }
                handler()
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in task.cancel() }
        }
    }
}
