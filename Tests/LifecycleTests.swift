// Deterministic tests for lifecycle, cancellation, and maxConcurrent flatMap semantics.

import Testing
@testable import AsyncRay

// MARK: - AsyncRayEmitter onTermination / onCancellation / onFinishOrCancel

@Test func onFinishOrCancelFiresOnNormalFinish() async {
    let fired = Collector<Bool>()
    let asyncRay = AsyncRay<Int> { emitter in
        emitter.onFinishOrCancel { fired.append(true) }
        emitter.send(1)
        emitter.finish()
    }
    _ = await asyncRay.collect()
    try? await Task.sleep(for: .milliseconds(20))
    #expect(fired.values == [true])
}

@Test func onFinishOrCancelFiresOnCancellation() async {
    let fired = Collector<Bool>()
    let asyncRay = AsyncRay<Int> { emitter in
        emitter.onFinishOrCancel { fired.append(true) }
    }
    let sub = asyncRay.sink { _ in }
    try? await Task.sleep(for: .milliseconds(10))
    sub.cancel()
    try? await Task.sleep(for: .milliseconds(20))
    #expect(fired.values == [true])
}

@Test func onCancellationDoesNotFireOnNormalFinish() async {
    let cancelled = Collector<Bool>()
    let asyncRay = AsyncRay<Int> { emitter in
        emitter.onCancellation { cancelled.append(true) }
        emitter.send(1)
        emitter.finish()
    }
    _ = await asyncRay.collect()
    try? await Task.sleep(for: .milliseconds(20))
    #expect(cancelled.values.isEmpty)
}

@Test func onTerminationHandlerCalledExactlyOnce() async {
    let calls = Collector<Int>()
    let asyncRay = AsyncRay<Int> { emitter in
        emitter.onTermination { _ in calls.append(1) }
        emitter.send(1)
        emitter.finish()
    }
    _ = await asyncRay.collect()
    try? await Task.sleep(for: .milliseconds(20))
    #expect(calls.count() == 1)
}

/// Handler registered AFTER the subscription was already cancelled (simulates gap after async socket opening).
/// Must still receive terminal reason immediately.
@Test func lateRegisteredHandlerReceivesAlreadyFiredTermination() async {
    let fired = Collector<Bool>()
    let asyncRay = AsyncRay<Int> { emitter in
        Task {
            try? await Task.sleep(for: .milliseconds(30))
            emitter.onFinishOrCancel { fired.append(true) }
        }
    }
    let sub = asyncRay.sink { _ in }
    try? await Task.sleep(for: .milliseconds(5))
    sub.cancel()
    try? await Task.sleep(for: .milliseconds(60))
    #expect(fired.values == [true])
}

@Test func multipleIndependentHandlersAllFire() async {
    let a = Collector<Bool>()
    let b = Collector<Bool>()
    let asyncRay = AsyncRay<Int> { emitter in
        emitter.onFinishOrCancel { a.append(true) }
        emitter.onFinishOrCancel { b.append(true) }
        emitter.send(1)
        emitter.finish()
    }
    _ = await asyncRay.collect()
    try? await Task.sleep(for: .milliseconds(20))
    #expect(a.values == [true])
    #expect(b.values == [true])
}

// MARK: - debounce cancellation

@Test func debounceDoesNotDeliverAfterSubscriptionCancelled() async {
    let pipe = Pipe<Int>()
    let results = Collector<Int>()

    let sub = pipe.asyncRay
        .map { $0 }
        .filter { _ in true }
        .debounce(.milliseconds(50))
        .sink { v in results.append(v) }

    try? await Task.sleep(for: .milliseconds(10))
    pipe.send(1)

    try? await Task.sleep(for: .milliseconds(10))
    sub.cancel()

    try? await Task.sleep(for: .milliseconds(100))
    let all = results.values
    #expect(all.isEmpty)
}

// MARK: - timeout cancellation

@Test func timeoutCompletesDownstreamAfterDuration() async {
    let pipe = Pipe<Int>()
    let completed = Collector<Bool>()

    let sub = pipe.asyncRay
        .timeout(.milliseconds(40))
        .sink(
            next: { _ in },
            completed: { completed.append(true) }
        )

    try? await Task.sleep(for: .milliseconds(120))
    sub.cancel()

    #expect(completed.values == [true])
}

@Test func timeoutSourceCleanupRunsExactlyOnce() async {
    let cleanupCount = Collector<Int>()

    let source = AsyncRay<Int> { emitter in
        emitter.onFinishOrCancel { cleanupCount.append(1) }
        emitter.send(1)
    }

    let sub = source.timeout(.milliseconds(30)).sink { _ in }
    try? await Task.sleep(for: .milliseconds(100))
    sub.cancel()
    try? await Task.sleep(for: .milliseconds(20))

    #expect(cleanupCount.count() == 1)
}

// MARK: - flatMapLatest cancellation

@Test func flatMapLatestCancelsActiveInnerOnSubscriptionCancel() async {
    let pipe = Pipe<Int>()
    let innerCleanups = Collector<Int>()

    let sub = pipe.asyncRay
        .flatMapLatest(bufferingPolicy: .unbounded) { n -> AsyncRay<Int> in
            AsyncRay<Int> { emitter in
                emitter.onCancellation { innerCleanups.append(n) }
            }
        }
        .sink { _ in }

    try? await Task.sleep(for: .milliseconds(10))
    pipe.send(1)
    try? await Task.sleep(for: .milliseconds(20))
    sub.cancel()
    try? await Task.sleep(for: .milliseconds(20))

    #expect(innerCleanups.values == [1])
}

// MARK: - flatMap(maxConcurrent:)

actor ConcurrencyWatermark {
    private var active = 0
    private var maxSeen = 0

    func enter() -> Int {
        active += 1
        maxSeen = max(maxSeen, active)
        return active
    }

    func exit() {
        active -= 1
    }

    var observedMax: Int { maxSeen }
}

@Test func flatMapNeverExceedsMaxConcurrent() async {
    let watermark = ConcurrencyWatermark()

    let makeInner: @Sendable (Int) -> AsyncRay<Int> = { n in
        AsyncRay<Int> { emitter in
            Task {
                _ = await watermark.enter()
                try? await Task.sleep(for: .milliseconds(20))
                emitter.send(n)
                await watermark.exit()
                emitter.finish()
            }
        }
    }

    let values = await AsyncRay.from(Array(1...8))
        .flatMap(maxConcurrent: 2, bufferingPolicy: .bufferingNewest(64)) { makeInner($0) }
        .collect()

    #expect(values.count == 8)
    #expect(await watermark.observedMax <= 2)
}

@Test func flatMapWaitsForRunningInnersBeforeCompleting() async {
    let sawAllValues = await AsyncRay.from([1, 2, 3])
        .flatMap(maxConcurrent: 3, bufferingPolicy: .bufferingNewest(64)) { n in
            AsyncRay<Int> { emitter in
                Task {
                    try? await Task.sleep(for: .milliseconds(20))
                    emitter.send(n * 10)
                    emitter.finish()
                }
            }
        }
        .collect()

    #expect(Set(sawAllValues) == Set([10, 20, 30]))
}

// MARK: - Order guarantees

@Test func sequentialOperatorsPreserveOrder() async {
    let values = await AsyncRay.from(Array(1...20))
        .map { $0 * 2 }
        .filter { $0 % 4 == 0 }
        .take(3)
        .collect()
    #expect(values == [4, 8, 12])
}

@Test func mergeDoesNotAssumeOrderBetweenSources() async {
    let a = AsyncRay.from([1, 2, 3])
    let b = AsyncRay.from([4, 5, 6])
    let values = await merge([a, b], bufferingPolicy: .bufferingNewest(64)).collect()
    #expect(Set(values) == Set([1, 2, 3, 4, 5, 6]))
}

// MARK: - SharedAsyncRay / AsyncStream.asAsyncRay() Lifecycle

@Test func asAsyncRayDoesNotReadUpstreamWithoutSubscription() async {
    let readStarted = Collector<Bool>()
    let stream = AsyncStream<Int>(unfolding: {
        readStarted.append(true)
        return nil
    })

    let asyncRay = stream.asAsyncRay()
    try? await Task.sleep(for: .milliseconds(20))

    let started = readStarted.values
    #expect(started.isEmpty)
    let _ = asyncRay
}

@Test func asAsyncRayCancelsPumpOnLastSubscriberDisconnect() async {
    let upstreamCancelled = Collector<Bool>()
    let stream = AsyncStream<Int> { continuation in
        continuation.onTermination = { @Sendable _ in
            upstreamCancelled.append(true)
        }
    }

    let asyncRay = stream.asAsyncRay()
    let sub = asyncRay.sink { _ in }

    try? await Task.sleep(for: .milliseconds(20))
    sub.cancel()
    try? await Task.sleep(for: .milliseconds(30))

    let wasCancelled = upstreamCancelled.values
    #expect(wasCancelled == [true])
}

@Test func asAsyncRayMulticastsToMultipleSubscribers() async {
    let stream = AsyncStream<Int> { continuation in
        Task {
            continuation.yield(100)
            continuation.yield(200)
            continuation.finish()
        }
    }

    let asyncRay = stream.asAsyncRay()
    let r1 = Collector<Int>()
    let r2 = Collector<Int>()

    let s1 = asyncRay.sink { v in r1.append(v) }
    let s2 = asyncRay.sink { v in r2.append(v) }

    try? await Task.sleep(for: .milliseconds(40))
    s1.cancel()
    s2.cancel()

    let v1 = r1.values
    let v2 = r2.values
    #expect(v1 == [100, 200])
    #expect(v2 == [100, 200])
}

@Test func asAsyncRaySubsequentSubscribersAfterTerminalReceiveEmpty() async {
    let stream = AsyncStream<Int> { continuation in
        continuation.yield(1)
        continuation.finish()
    }

    let asyncRay = stream.asAsyncRay()
    let first = await asyncRay.collect()
    #expect(first == [1])

    // After terminal state, subsequent subscribers receive empty completed stream
    let second = await asyncRay.collect()
    #expect(second.isEmpty)
}

