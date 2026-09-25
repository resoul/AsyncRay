import Testing
import Foundation
@testable import AsyncRay

// MARK: - scan

@Test func scanRunningSum() async {
    let result = await AsyncRay.from([1, 2, 3, 4, 5])
        .scan(0) { acc, next in acc + next }
        .collect()
    #expect(result == [1, 3, 6, 10, 15])
}

@Test func scanStringAccumulation() async {
    let result = await AsyncRay.from(["a", "b", "c"])
        .scan("") { acc, next in acc + next }
        .collect()
    #expect(result == ["a", "ab", "abc"])
}

@Test func scanIndependentPerSubscription() async {
    let source = AsyncRay.from([1, 2, 3])
    let scanned = source.scan(10) { acc, next in acc + next }

    let r1 = await scanned.collect()
    let r2 = await scanned.collect()

    #expect(r1 == [11, 13, 16])
    #expect(r2 == [11, 13, 16])
}

@Test func scanEmptyStream() async {
    let result = await AsyncRay<Int>.empty()
        .scan(100) { acc, next in acc + next }
        .collect()
    #expect(result.isEmpty)
}

@Test func scanCancellationStopsDelivery() async {
    let pipe = Pipe<Int>()
    let results = Collector<Int>()

    let sub = pipe.asyncRay
        .scan(0) { acc, next in acc + next }
        .sink { v in results.append(v) }

    try? await Task.sleep(for: .milliseconds(10))
    pipe.send(1)
    pipe.send(2)
    try? await Task.sleep(for: .milliseconds(20))

    sub.cancel()
    pipe.send(3)
    try? await Task.sleep(for: .milliseconds(20))

    let all = results.values
    #expect(all == [1, 3])
}

// MARK: - reduce

@Test func reduceSum() async {
    let result = await AsyncRay.from([1, 2, 3, 4])
        .reduce(0) { acc, next in acc + next }
        .collect()
    #expect(result == [10])
}

@Test func reduceEmptyStreamEmitsInitial() async {
    let result = await AsyncRay<Int>.empty()
        .reduce(42) { acc, next in acc + next }
        .collect()
    #expect(result == [42])
}

@Test func reduceEmitsSingleFinalValue() async {
    let pipe = Pipe<Int>()
    let results = Collector<Int>()

    let sub = pipe.asyncRay
        .reduce(0) { acc, next in acc + next }
        .sink { v in results.append(v) }

    pipe.send(10)
    pipe.send(20)
    try? await Task.sleep(for: .milliseconds(20))

    // Intermediate values should not be emitted
    let beforeFinish = results.values
    #expect(beforeFinish.isEmpty)

    pipe.finish()
    try? await Task.sleep(for: .milliseconds(20))

    let afterFinish = results.values
    #expect(afterFinish == [30])

    sub.cancel()
}

@Test func reduceNoEmissionOnCancellation() async {
    let pipe = Pipe<Int>()
    let results = Collector<Int>()

    let sub = pipe.asyncRay
        .reduce(0) { acc, next in acc + next }
        .sink { v in results.append(v) }

    pipe.send(10)
    pipe.send(20)
    try? await Task.sleep(for: .milliseconds(20))

    sub.cancel()
    pipe.finish()
    try? await Task.sleep(for: .milliseconds(20))

    let all = results.values
    #expect(all.isEmpty)
}
