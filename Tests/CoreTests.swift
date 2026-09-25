import Testing
import Foundation
@testable import AsyncRay

// MARK: - AsyncRay.just

@Test func justEmitsSingleValue() async {
    let values = await AsyncRay.just(42).collect()
    #expect(values == [42])
}

@Test func justCompletesAfterValue() async {
    var count = 0
    for await _ in AsyncRay.just("x").stream { count += 1 }
    #expect(count == 1)
}

// MARK: - AsyncRay.empty

@Test func emptyCompletesImmediately() async {
    let values = await AsyncRay<Int>.empty().collect()
    #expect(values == [])
}

// MARK: - AsyncRay.never

@Test func neverDoesNotEmitAndCanBeCancelled() async {
    let received = Collector<Int>()
    let sub = AsyncRay<Int>.never().sink { v in
        received.append(v)
    }
    
    try? await Task.sleep(for: .milliseconds(30))
    sub.cancel()
    
    let values = received.values
    #expect(values.isEmpty)
}

// MARK: - AsyncRay.timer

@Test func timerCompletesAfterDuration() async {
    let start = ContinuousClock.now
    let values = await AsyncRay<Int>.timer(.milliseconds(40)).collect()
    let elapsed = start.duration(to: .now)
    
    #expect(values.isEmpty)
    #expect(elapsed >= .milliseconds(30))
}

@Test func timerCancellationStopsTaskAndDoesNotComplete() async {
    let completed = Collector<Bool>()
    let sub = AsyncRay<Int>.timer(.milliseconds(100)).sink(
        next: { _ in },
        completed: { completed.append(true) }
    )

    try? await Task.sleep(for: .milliseconds(20))
    sub.cancel()
    try? await Task.sleep(for: .milliseconds(120))

    let fired = completed.values
    #expect(fired.isEmpty)
}

@Test func timerMultipleSubscriptionsIndependentCancellation() async {
    let timer = AsyncRay<Int>.timer(.milliseconds(50))
    let comp1 = Collector<Bool>()
    let comp2 = Collector<Bool>()

    let sub1 = timer.sink(
        next: { _ in },
        completed: { comp1.append(true) }
    )
    let sub2 = timer.sink(
        next: { _ in },
        completed: { comp2.append(true) }
    )

    try? await Task.sleep(for: .milliseconds(10))
    sub1.cancel()
    try? await Task.sleep(for: .milliseconds(70))
    sub2.cancel()

    #expect(comp1.values.isEmpty)
    #expect(comp2.values == [true])
}

// MARK: - AsyncRay.from

@Test func fromEmitsAllValues() async {
    let values = await AsyncRay.from([1, 2, 3]).collect()
    #expect(values == [1, 2, 3])
}

@Test func fromEmptySequence() async {
    let values = await AsyncRay<String>.from([]).collect()
    #expect(values.isEmpty)
}

// MARK: - AsyncRay.init(build:)

@Test func customEmitterSendsValues() async {
    let asyncRay = AsyncRay<Int> { emitter in
        emitter.send(10)
        emitter.send(20)
        emitter.send(30)
        emitter.finish()
    }
    let values = await asyncRay.collect()
    #expect(values == [10, 20, 30])
}

@Test func customEmitterAsync() async {
    let asyncRay = AsyncRay<String> { emitter in
        Task {
            emitter.send("hello")
            emitter.finish()
        }
    }
    let value = await asyncRay.first()
    #expect(value == "hello")
}

// MARK: - AsyncRay.first()

@Test func firstReturnsFirstValue() async {
    let value = await AsyncRay.from([1, 2, 3]).first()
    #expect(value == 1)
}

@Test func firstReturnsNilForEmpty() async {
    let value = await AsyncRay<Int>.empty().first()
    #expect(value == nil)
}

// MARK: - AsyncRay.stream (Each access creates a new subscription)

@Test func streamIsIndependent() async {
    let counter = Collector<Int>()
    let results = Collector<Int>()

    let asyncRay = AsyncRay<Int> { emitter in
        Task {
            let c = counter.count()
            counter.append(c + 1)
            let current = counter.count()
            emitter.send(current)
            emitter.finish()
        }
    }
    // Each .stream access triggers a new generator invocation
    for await v in asyncRay.stream { results.append(v) }
    for await v in asyncRay.stream { results.append(v) }

    let all = results.values
    let total = counter.count()
    #expect(all == [1, 2])
    #expect(total == 2)
}

// MARK: - AsyncRay: AsyncSequence conformance

@Test func asyncSequenceDirectIteration() async {
    let asyncRay = AsyncRay.from([10, 20, 30])
    var collected: [Int] = []
    for await value in asyncRay {
        collected.append(value)
    }
    #expect(collected == [10, 20, 30])
}

// MARK: - AsyncRay: throwingStream

@Test func throwingStreamConvertsAndCancels() async {
    let asyncRay = AsyncRay.from([1, 2, 3])
    var collected: [Int] = []
    
    do {
        for try await value in asyncRay.throwingStream {
            collected.append(value)
        }
    } catch {
        Issue.record("Should not throw: \(error)")
    }
    #expect(collected == [1, 2, 3])
}

@Test func throwingStreamCancellation() async {
    let pipe = Pipe<Int>()
    let stream = pipe.asyncRay.throwingStream
    let task = Task {
        var results: [Int] = []
        for try await v in stream {
            results.append(v)
        }
        return results
    }
    
    try? await Task.sleep(for: .milliseconds(10))
    pipe.send(1)
    try? await Task.sleep(for: .milliseconds(10))
    task.cancel()
    pipe.send(2)
    
    let res = (try? await task.value) ?? []
    #expect(res == [1])
}

// MARK: - Buffering Policy Inheritance

@Test func bufferingPolicyInheritanceOldest() async {
    let asyncRay = AsyncRay<Int>(bufferingPolicy: .bufferingOldest(2)) {
        AsyncStream<Int>(bufferingPolicy: .bufferingOldest(2)) { continuation in
            continuation.yield(1)
            continuation.yield(2)
            continuation.yield(3)
            continuation.finish()
        }
    }
    
    let mapped = asyncRay.map { $0 * 2 }
    let values = await mapped.collect()
    #expect(values == [2, 4])
}

@Test func bufferingPolicyEquatable() {
    let p1: _AsyncRayBufferingPolicy = .unbounded
    let p2: _AsyncRayBufferingPolicy = .unbounded
    let p3: _AsyncRayBufferingPolicy = .bufferingNewest(64)
    let p4: _AsyncRayBufferingPolicy = .bufferingNewest(64)
    let p5: _AsyncRayBufferingPolicy = .bufferingOldest(32)

    #expect(p1 == p2)
    #expect(p3 == p4)
    #expect(p1 != p3)
    #expect(p3 != p5)
}

// MARK: - Subscription.cancel() and sink(next:completed:)

@Test func cancellationStopsDelivery() async {
    let received = Collector<Int>()

    let pipe = Pipe<Int>()
    let sub = pipe.asyncRay.sink { value in
        received.append(value)
    }

    pipe.send(1)
    pipe.send(2)

    try? await Task.sleep(for: .milliseconds(20))
    let beforeCancel = received.values
    #expect(beforeCancel == [1, 2])

    sub.cancel()
    pipe.send(3)

    try? await Task.sleep(for: .milliseconds(20))
    let afterCancel = received.values
    #expect(afterCancel == [1, 2])  // 3 was not delivered
}

@Test func sinkWithCompletionCancellation() async {
    let completedFired = Collector<Bool>()
    let pipe = Pipe<Int>()
    
    let sub = pipe.asyncRay.sink(
        next: { _ in },
        completed: { completedFired.append(true) }
    )
    
    try? await Task.sleep(for: .milliseconds(10))
    sub.cancel()
    pipe.finish()
    try? await Task.sleep(for: .milliseconds(20))
    
    let fired = completedFired.values
    #expect(fired.isEmpty)
}

// MARK: - SubscriptionBag

@Test func bagCancelsAllOnDeinit() async {
    let received = Collector<Int>()
    let pipe = Pipe<Int>()

    do {
        let bag = SubscriptionBag()
        pipe.asyncRay.sink { value in
            received.append(value)
        }.store(in: bag)
        pipe.send(1)
        try? await Task.sleep(for: .milliseconds(20))
        // bag goes out of scope → deinit → cancel
    }

    pipe.send(2)
    try? await Task.sleep(for: .milliseconds(20))
    let all = received.values
    #expect(all == [1])
}

@Test func bagCancelAllManually() async {
    let received = Collector<Int>()
    let pipe = Pipe<Int>()
    let bag = SubscriptionBag()
    
    pipe.asyncRay.sink { value in
        received.append(value)
    }.store(in: bag)
    
    pipe.send(1)
    try? await Task.sleep(for: .milliseconds(20))
    
    bag.cancelAll()
    pipe.send(2)
    try? await Task.sleep(for: .milliseconds(20))
    
    let all = received.values
    #expect(all == [1])
}

@Test func bagAutoPrunesCompletedSubscriptions() async {
    let bag = SubscriptionBag()

    AsyncRay.from([1, 2, 3]).sink { _ in }.store(in: bag)
    try? await Task.sleep(for: .milliseconds(20))

    #expect(bag.count == 0)
}

@Test func bagAutoPrunesCancelledSubscriptions() async {
    let bag = SubscriptionBag()
    let pipe = Pipe<Int>()

    let sub = pipe.asyncRay.sink { _ in }
    sub.store(in: bag)

    #expect(bag.count == 1)

    sub.cancel()
    try? await Task.sleep(for: .milliseconds(10))

    #expect(bag.count == 0)
}

@Test func bagDoubleCancelIsSafe() async {
    let bag = SubscriptionBag()
    let pipe = Pipe<Int>()

    let sub = pipe.asyncRay.sink { _ in }
    sub.store(in: bag)

    sub.cancel()
    sub.cancel()
    bag.cancelAll()

    #expect(bag.count == 0)
}