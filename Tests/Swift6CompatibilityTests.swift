import Testing
@testable import AsyncRay

@Test(.timeLimit(.minutes(1)))
func test_zip_rightCompletesWithoutValue_cancelsPendingLeft() async {
    let left = AsyncStream<Int>.makeStream(bufferingPolicy: .bufferingNewest(1))
    let cancelled = AsyncStream<Bool>.makeStream(bufferingPolicy: .bufferingNewest(1))
    left.continuation.onTermination = { _ in cancelled.continuation.yield(true); cancelled.continuation.finish() }
    let values = await zip(AsyncRay { left.stream }, AsyncRay<String>.empty(), bufferingPolicy: .bufferingNewest(1)).collect()
    #expect(values.isEmpty)
    #expect(await AsyncRay { cancelled.stream }.first() == true)
}

@Test(.timeLimit(.minutes(1)))
func test_zip_asymmetricFiniteStreams_preservesPairs() async {
    let values = await zip(AsyncRay.from(Array(0..<128)), AsyncRay.from(Array(1000..<1040)), bufferingPolicy: .bufferingNewest(128)).collect()
    #expect(values.map(\.0) == Array(0..<40))
    #expect(values.map(\.1) == Array(1000..<1040))
}

@Test(.timeLimit(.minutes(1)))
func test_zip_cancelWhileOneSideWaits_releasesBothSources() async {
    let left = AsyncStream<Int>.makeStream(bufferingPolicy: .bufferingNewest(1))
    let right = AsyncStream<String>.makeStream(bufferingPolicy: .bufferingNewest(1))
    let leftCancelled = AsyncStream<Bool>.makeStream(bufferingPolicy: .bufferingNewest(1))
    let rightCancelled = AsyncStream<Bool>.makeStream(bufferingPolicy: .bufferingNewest(1))
    left.continuation.onTermination = { _ in leftCancelled.continuation.yield(true); leftCancelled.continuation.finish() }
    right.continuation.onTermination = { _ in rightCancelled.continuation.yield(true); rightCancelled.continuation.finish() }
    left.continuation.yield(1)
    // Direct stream wrappers avoid the unrelated asynchronous shared-bridge registration path.
    let stream = zip(AsyncRay { left.stream }, AsyncRay { right.stream }, bufferingPolicy: .bufferingNewest(1)).stream
    let consumer = Task { for await _ in stream {} }
    consumer.cancel()
    await consumer.value
    #expect(await AsyncRay { leftCancelled.stream }.first() == true)
    #expect(await AsyncRay { rightCancelled.stream }.first() == true)
}

@Test(.timeLimit(.minutes(1)))
func test_sharedAsyncRay_registeredSubscribers_receiveSinglePumpAndTerminal() async {
    let input = AsyncStream<Int>.makeStream(bufferingPolicy: .bufferingNewest(2))
    let shared = SharedAsyncRay(input.stream)
    let first = AsyncStream<Int>.makeStream(bufferingPolicy: .bufferingNewest(2))
    let second = AsyncStream<Int>.makeStream(bufferingPolicy: .bufferingNewest(2))
    await shared.registerSubscriber(id: .init(), continuation: first.continuation)
    await shared.registerSubscriber(id: .init(), continuation: second.continuation)
    input.continuation.yield(1)
    input.continuation.yield(2)
    input.continuation.finish()
    var a: [Int] = []
    for await value in first.stream { a.append(value) }
    var b: [Int] = []
    for await value in second.stream { b.append(value) }
    #expect(a == [1, 2])
    #expect(b == [1, 2])
    #expect(await shared.subscriberCount == 0)
    #expect(await shared.asyncRay.collect().isEmpty)
}
