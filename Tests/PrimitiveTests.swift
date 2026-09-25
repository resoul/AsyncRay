import Testing
@testable import AsyncRay

// MARK: - Pipe

@Test func pipeSendsToMultipleSubscribers() async {
    let pipe = Pipe<Int>()
    let r1 = Collector<Int>()
    let r2 = Collector<Int>()

    let s1 = pipe.asyncRay.sink { v in r1.append(v) }
    let s2 = pipe.asyncRay.sink { v in r2.append(v) }

    pipe.send(1)
    pipe.send(2)
    try? await Task.sleep(for: .milliseconds(20))

    let all1 = r1.values
    let all2 = r2.values
    #expect(all1 == [1, 2])
    #expect(all2 == [1, 2])

    s1.cancel()
    s2.cancel()
}

@Test func pipeOperatorSendsValue() async {
    let pipe = Pipe<String>()
    let results = Collector<String>()
    let sub = pipe.asyncRay.sink { v in results.append(v) }

    try? await Task.sleep(for: .milliseconds(20)) // Wait for subscriber registration
    pipe <- "hello"
    pipe <- "world"
    try? await Task.sleep(for: .milliseconds(40))
    sub.cancel()

    #expect(results.values == ["hello", "world"])
}

@Test func pipeOperatorSendsArray() async {
    let pipe = Pipe<Int>()
    let results = Collector<Int>()
    let sub = pipe.asyncRay.sink { v in results.append(v) }

    try? await Task.sleep(for: .milliseconds(25)) // Wait for subscriber registration
    pipe <- [1, 2, 3]
    try? await Task.sleep(for: .milliseconds(40))
    sub.cancel()

    #expect(results.values == [1, 2, 3])
}

@Test func pipeFinishNotifiesSubscribers() async {
    let pipe = Pipe<Int>()
    let completed = Collector<Bool>()

    let sub = pipe.asyncRay.sink(
        next: { _ in },
        completed: { completed.append(true) }
    )
    pipe.finish()
    try? await Task.sleep(for: .milliseconds(20))
    sub.cancel()

    #expect(completed.values == [true])
}

@Test func pipeFinishTwiceIsSafe() async {
    let pipe = Pipe<Int>()
    pipe.finish()
    pipe.finish() // Should not crash or misbehave
    
    // Subscribing after finish
    let values = await pipe.asyncRay.collect()
    #expect(values.isEmpty)
}

@Test func pipeLateSubscriberMissesValues() async {
    let pipe = Pipe<Int>()
    pipe.send(1)  // Sent prior to subscription — should not be received by hot pipe

    let results = Collector<Int>()
    let sub = pipe.asyncRay.sink { v in results.append(v) }

    try? await Task.sleep(for: .milliseconds(10))
    pipe.send(2)  // Sent after subscription
    try? await Task.sleep(for: .milliseconds(20))
    sub.cancel()

    #expect(results.values == [2])
}

@Test func pipeDirectStreamProperty() async {
    let pipe = Pipe<Int>()
    let stream = pipe.stream
    
    try? await Task.sleep(for: .milliseconds(10))
    pipe.send(100)
    pipe.finish()
    
    var collected: [Int] = []
    for await val in stream {
        collected.append(val)
    }
    #expect(collected == [100])
}

@Test func pipeSubscriberCount() async {
    let pipe = Pipe<Int>()
    #expect(pipe.subscriberCount == 0)
    
    let sub1 = pipe.asyncRay.sink { _ in }
    try? await Task.sleep(for: .milliseconds(20))
    #expect(pipe.subscriberCount == 1)
    
    let sub2 = pipe.asyncRay.sink { _ in }
    try? await Task.sleep(for: .milliseconds(20))
    #expect(pipe.subscriberCount == 2)
    
    sub1.cancel()
    try? await Task.sleep(for: .milliseconds(20))
    #expect(pipe.subscriberCount == 1)
    
    sub2.cancel()
    try? await Task.sleep(for: .milliseconds(20))
    #expect(pipe.subscriberCount == 0)
}

// MARK: - Pipe bounded buffering

@Test func pipeBufferingNewestKeepsLatestOnOverflow() async {
    let pipe = Pipe<Int>(bufferingPolicy: .bufferingNewest(2))
    let stream = pipe.asyncRay.stream

    try? await Task.sleep(for: .milliseconds(10))

    for i in 1...5 {
        pipe.send(i)
    }

    var iterator = stream.makeAsyncIterator()
    let first = await iterator.next()
    let second = await iterator.next()
    #expect([first, second] == [4, 5])

    pipe.finish()
    let third = await iterator.next()
    #expect(third == nil)
}

@Test func pipeSendObservingOverflowReportsResultPerSubscriber() async {
    let pipe = Pipe<Int>(bufferingPolicy: .bufferingNewest(1))
    let stream = pipe.asyncRay.stream
    try? await Task.sleep(for: .milliseconds(10))

    let firstResults = pipe.sendObservingOverflow(1)
    #expect(firstResults.count == 1)
    let secondResults = pipe.sendObservingOverflow(2)
    #expect(secondResults.count == 1)

    for r in firstResults + secondResults {
        if case .terminated = r { Issue.record("stream should not be terminated yet") }
    }

    pipe.finish()
    var iterator = stream.makeAsyncIterator()
    let value = await iterator.next()
    #expect(value != nil)
}

@Test func pipeDefaultPolicyIsBufferingNewest64() async {
    let pipe = Pipe<Int>()
    let stream = pipe.asyncRay.stream
    try? await Task.sleep(for: .milliseconds(10))

    for i in 1...10 {
        pipe.send(i)
    }
    pipe.finish()

    var received: [Int] = []
    for await v in stream { received.append(v) }
    #expect(received == Array(1...10))
}

// MARK: - CurrentValue

@Test func currentValueImmediateReplay() async {
    let state = CurrentValue(42)
    let value = await state.asyncRay.first()
    #expect(value == 42)
}

@Test func currentValueNotifiesOnSet() async {
    let state = CurrentValue(0)
    let results = Collector<Int>()

    let sub = state.asyncRay.sink { v in results.append(v) }
    try? await Task.sleep(for: .milliseconds(10))

    await state.set(1)
    await state.set(2)
    try? await Task.sleep(for: .milliseconds(20))
    sub.cancel()

    let all = results.values
    #expect(all.first == 0)
    #expect(all.contains(1))
    #expect(all.contains(2))
}

@Test func currentValueDirectStream() async {
    let state = CurrentValue("init")
    let stream = state.stream
    
    var iterator = stream.makeAsyncIterator()
    let first = await iterator.next()
    #expect(first == "init")
}

@Test func currentValueCancellationCleansUp() async {
    let state = CurrentValue(1)
    let sub = state.asyncRay.sink { _ in }
    
    try? await Task.sleep(for: .milliseconds(20))
    sub.cancel()
    try? await Task.sleep(for: .milliseconds(20))
    
    await state.set(2)
    #expect(await state.value == 2)
}

@Test func currentValueLatestWinsUnderSlowConsumer() async {
    let state = CurrentValue(0)
    let stream = state.asyncRay.stream
    try? await Task.sleep(for: .milliseconds(10))

    await state.set(1)
    await state.set(2)
    await state.set(3)

    var iterator = stream.makeAsyncIterator()
    let first = await iterator.next()
    #expect(first == 0 || first == 3)
}

@Test func currentValueModify() async {
    let counter = CurrentValue(10)
    await counter.modify { $0 + 5 }
    #expect(await counter.value == 15)
}

// MARK: - CurrentValueDistinct

@Test func currentValueDistinctSkipsRepeats() async {
    let state = CurrentValueDistinct(0)
    let results = Collector<Int>()

    let sub = state.asyncRay.sink { v in results.append(v) }
    try? await Task.sleep(for: .milliseconds(10))

    await state.set(1)
    await state.set(1)  // Duplicate — skipped
    await state.set(2)
    try? await Task.sleep(for: .milliseconds(20))
    sub.cancel()

    let all = results.values
    #expect(all == [0, 1, 2])
}

@Test func currentValueDistinctModifyAndStream() async {
    let state = CurrentValueDistinct(10)
    #expect(await state.value == 10)
    
    await state.modify { $0 * 2 }
    #expect(await state.value == 20)
    
    let stream = state.stream
    var iterator = stream.makeAsyncIterator()
    let val = await iterator.next()
    #expect(val == 20)
}

@Test func currentValueDistinctCancellationCleansUp() async {
    let state = CurrentValueDistinct("a")
    let sub = state.asyncRay.sink { _ in }
    
    try? await Task.sleep(for: .milliseconds(20))
    sub.cancel()
    try? await Task.sleep(for: .milliseconds(20))
    
    await state.set("b")
    #expect(await state.value == "b")
}

// MARK: - Once

@Test func onceResolveBeforeSubscribe() async {
    let once = Once<String>()
    await once.resolve("done")

    let value = await once.wait()
    #expect(value == "done")
}

@Test func onceValueAwaitsResolution() async throws {
    let once = Once<Int>()
    async let pending = once.value
    try? await Task.sleep(for: .milliseconds(10))
    await once.resolve(7)
    #expect(try await pending == 7)
    #expect(try await once.value == 7)
}

@Test func onceValueThrowsForAlreadyCancelledCaller() async {
    let once = Once<Int>()
    let waiter = Task {
        withUnsafeCurrentTask { $0?.cancel() }
        return try await once.value
    }
    let result = await waiter.result
    #expect(throws: CancellationError.self) { try result.get() }
}

@Test func onceResolveAfterSubscribe() async {
    let once = Once<Int>()
    let result = Collector<Int>()

    let sub = once.asyncRay.sink { v in result.append(v) }

    try? await Task.sleep(for: .milliseconds(10))
    await once.resolve(99)
    try? await Task.sleep(for: .milliseconds(20))
    sub.cancel()

    #expect(result.values == [99])
}

@Test func onceResolvesOnlyOnce() async {
    let once = Once<Int>()
    await once.resolve(1)
    await once.resolve(2)  // Ignored

    let value = await once.wait()
    #expect(value == 1)
}

@Test func onceMultipleWaiters() async {
    let once = Once<String>()
    let results = Collector<String>()

    async let w1 = once.wait()
    async let w2 = once.wait()
    async let w3 = once.wait()

    await once.resolve("ready")

    let (v1, v2, v3) = await (w1, w2, w3)
    #expect(v1 == "ready")
    #expect(v2 == "ready")
    #expect(v3 == "ready")
    let _ = results
}

@Test func onceCurrentValueBeforeResolve() async {
    let once = Once<Bool>()
    #expect(await once.currentValue == nil)
    await once.resolve(true)
    #expect(await once.currentValue == true)
}

@Test func onceStreamProperty() async {
    let once = Once<Int>()
    let stream = once.stream
    
    Task {
        try? await Task.sleep(for: .milliseconds(10))
        await once.resolve(42)
    }
    
    var iterator = stream.makeAsyncIterator()
    let val = await iterator.next()
    #expect(val == 42)
}

@Test func onceCancellationRemovesWaiterWithoutWaitingResolve() async {
    let once = Once<Int>()
    let result = Collector<Int>()

    let sub = once.asyncRay.sink { v in result.append(v) }
    try? await Task.sleep(for: .milliseconds(10))

    sub.cancel()
    try? await Task.sleep(for: .milliseconds(10))

    await once.resolve(100)
    try? await Task.sleep(for: .milliseconds(20))

    let values = result.values
    #expect(values.isEmpty)
}

@Test func onceCancelOneWaiterLeavesOthersActive() async {
    let once = Once<Int>()
    let r1 = Collector<Int>()
    let r2 = Collector<Int>()

    let sub1 = once.asyncRay.sink { v in r1.append(v) }
    let sub2 = once.asyncRay.sink { v in r2.append(v) }

    try? await Task.sleep(for: .milliseconds(10))
    sub1.cancel()

    await once.resolve(777)
    try? await Task.sleep(for: .milliseconds(20))
    sub2.cancel()

    #expect(r1.values.isEmpty)
    #expect(r2.values == [777])
}

@Test func onceConcurrentResolvesPreserveFirstValue() async {
    let once = Once<Int>()
    await withTaskGroup(of: Void.self) { group in
        group.addTask { await once.resolve(1) }
        group.addTask { await once.resolve(2) }
    }

    let val = await once.wait()
    #expect(val == 1 || val == 2)
    #expect(await once.currentValue == val)
}

