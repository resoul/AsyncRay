import Testing
import Foundation
@testable import AsyncRay

// MARK: - combineLatest 3 streams

@Test func combineLatestThreeStreams() async {
    let p1 = Pipe<Int>()
    let p2 = Pipe<String>()
    let p3 = Pipe<Bool>()
    let results = Collector<(Int, String, Bool)>()

    let sub = combineLatest(
        p1.asyncRay,
        p2.asyncRay,
        p3.asyncRay,
        bufferingPolicy: .bufferingNewest(64)
    )
    .sink { tuple in results.append(tuple) }

    try? await Task.sleep(for: .milliseconds(10))
    p1.send(1)
    p2.send("a")
    try? await Task.sleep(for: .milliseconds(20))
    #expect(results.values.isEmpty)  // p3 has not emitted yet

    p3.send(true)
    try? await Task.sleep(for: .milliseconds(20))
    let first = results.values
    #expect(first.count == 1)
    #expect(first.first?.0 == 1 && first.first?.1 == "a" && first.first?.2 == true)

    p2.send("b")
    try? await Task.sleep(for: .milliseconds(20))
    let second = results.values
    #expect(second.count == 2)
    #expect(second.last?.0 == 1 && second.last?.1 == "b" && second.last?.2 == true)

    p1.send(2)
    try? await Task.sleep(for: .milliseconds(20))
    let third = results.values
    #expect(third.count == 3)
    #expect(third.last?.0 == 2 && third.last?.1 == "b" && third.last?.2 == true)

    sub.cancel()
}

// MARK: - combineLatest 4 streams

@Test func combineLatestFourStreams() async {
    let p1 = Pipe<Int>()
    let p2 = Pipe<String>()
    let p3 = Pipe<Bool>()
    let p4 = Pipe<Double>()
    let results = Collector<(Int, String, Bool, Double)>()

    let sub = combineLatest(
        p1.asyncRay,
        p2.asyncRay,
        p3.asyncRay,
        p4.asyncRay,
        bufferingPolicy: .bufferingNewest(64)
    )
    .sink { tuple in results.append(tuple) }

    try? await Task.sleep(for: .milliseconds(10))
    p1.send(1)
    p2.send("x")
    p3.send(false)
    try? await Task.sleep(for: .milliseconds(20))
    #expect(results.values.isEmpty)

    p4.send(3.14)
    try? await Task.sleep(for: .milliseconds(20))
    let afterAll = results.values
    #expect(afterAll.count == 1)
    #expect(
        afterAll.first?.0 == 1 && afterAll.first?.1 == "x" && afterAll.first?.2 == false
            && afterAll.first?.3 == 3.14
    )

    p4.send(2.71)
    try? await Task.sleep(for: .milliseconds(20))
    let afterP4 = results.values
    #expect(afterP4.count == 2)
    #expect(afterP4.last?.3 == 2.71)

    p3.send(true)
    try? await Task.sleep(for: .milliseconds(20))
    let afterP3 = results.values
    #expect(afterP3.count == 3)
    #expect(afterP3.last?.2 == true)

    sub.cancel()
}

// MARK: - combineLatest completion semantics (Finite + Pipe)

@Test func combineLatestFinitePlusInfinitePipeCompletionSemantics() async {
    let finite = AsyncRay.from([10, 20])
    let pipe = Pipe<String>()
    let results = Collector<(Int, String)>()

    let sub = combineLatest(finite, pipe.asyncRay, bufferingPolicy: .bufferingNewest(64))
        .sink { pair in results.append(pair) }

    try? await Task.sleep(for: .milliseconds(20))
    pipe.send("first")
    try? await Task.sleep(for: .milliseconds(20))

    let r1 = results.values
    #expect(r1.count == 1)
    #expect(r1[0].0 == 20 && r1[0].1 == "first")

    pipe.send("second")
    try? await Task.sleep(for: .milliseconds(20))

    let r2 = results.values
    #expect(r2.count == 2)
    #expect(r2[1].0 == 20 && r2[1].1 == "second")

    sub.cancel()
}

// MARK: - combineLatest deprecated overloads

@available(*, deprecated, message: "Exercises deprecated compatibility overloads.")
@Test func combineLatestDeprecatedOverloads() async {
    let f1 = AsyncRay.just(1)
    let f2 = AsyncRay.just("two")
    let f3 = AsyncRay.just(3.0)
    let f4 = AsyncRay.just(true)

    let res2 = await combineLatest(f1, f2).collect()
    #expect(res2.count == 1)
    #expect(res2[0].0 == 1 && res2[0].1 == "two")

    let res3 = await combineLatest(f1, f2, f3).collect()
    #expect(res3.count == 1)
    #expect(res3[0].0 == 1 && res3[0].1 == "two" && res3[0].2 == 3.0)

    let res4 = await combineLatest(f1, f2, f3, f4).collect()
    #expect(res4.count == 1)
    #expect(res4[0].0 == 1 && res4[0].1 == "two" && res4[0].2 == 3.0 && res4[0].3 == true)
}

// MARK: - zip

@available(*, deprecated, message: "Exercises deprecated compatibility overloads.")
@Test func zipDeprecatedUnbounded() async {
    let pairs = await zip(AsyncRay.from([1, 2, 3]), AsyncRay.from(["a", "b"])).collect()
    #expect(pairs.map { $0.0 } == [1, 2])
    #expect(pairs.map { $0.1 } == ["a", "b"])
}

@Test func zipPairsValuesSequentially() async {
    let f1 = AsyncRay.from([1, 2, 3, 4])
    let f2 = AsyncRay.from(["a", "b", "c"])

    let pairs = await zip(f1, f2, bufferingPolicy: .bufferingNewest(64)).collect()
    #expect(pairs.count == 3)
    #expect(pairs[0].0 == 1 && pairs[0].1 == "a")
    #expect(pairs[1].0 == 2 && pairs[1].1 == "b")
    #expect(pairs[2].0 == 3 && pairs[2].1 == "c")
}

@Test func zipReadsConcurrently() async {
    let bReadStarted = Collector<Bool>()

    let slowA = AsyncRay<Int> { emitter in
        Task {
            try? await Task.sleep(for: .milliseconds(60))
            emitter.send(1)
            emitter.finish()
        }
    }

    let fastB = AsyncRay<String> { emitter in
        Task {
            bReadStarted.append(true)
            emitter.send("b1")
            emitter.finish()
        }
    }

    let zipped = zip(slowA, fastB, bufferingPolicy: .bufferingNewest(16))
    let sub = zipped.sink { _ in }

    try? await Task.sleep(for: .milliseconds(20))
    // B should have been read before A was ready
    #expect(bReadStarted.values == [true])

    try? await Task.sleep(for: .milliseconds(70))
    sub.cancel()
}

@Test func zipEarlyCompletionOfOneSideCancelsOther() async {
    let bCancelled = Collector<Bool>()

    let emptyA = AsyncRay<Int>.empty()
    let slowB = AsyncRay<String> { emitter in
        emitter.onCancellation {
            bCancelled.append(true)
        }
        Task {
            try? await Task.sleep(for: .milliseconds(100))
            emitter.send("b")
            emitter.finish()
        }
    }

    let pairs = await zip(emptyA, slowB, bufferingPolicy: .bufferingNewest(16)).collect()
    #expect(pairs.isEmpty)

    try? await Task.sleep(for: .milliseconds(30))
    #expect(bCancelled.values == [true])
}

@Test func zipCancellation() async {
    let p1 = Pipe<Int>()
    let p2 = Pipe<String>()
    let results = Collector<(Int, String)>()

    let sub = zip(p1.asyncRay, p2.asyncRay, bufferingPolicy: .bufferingNewest(64)).sink { pair in
        results.append(pair)
    }

    try? await Task.sleep(for: .milliseconds(10))
    p1.send(1)
    p2.send("a")
    try? await Task.sleep(for: .milliseconds(20))

    sub.cancel()
    p1.send(2)
    p2.send("b")
    try? await Task.sleep(for: .milliseconds(20))

    let all = results.values
    #expect(all.count == 1)
}

// MARK: - merge extensions and edge cases

@Test func mergeEmptyArrayReturnsEmpty() async {
    let emptyMerge: AsyncRay<Int> = merge([], bufferingPolicy: .bufferingNewest(64))
    let values = await emptyMerge.collect()
    #expect(values.isEmpty)
}

@Test func mergeMemberExtension() async {
    let f1 = AsyncRay.from([1, 2])
    let f2 = AsyncRay.from([3, 4])

    let res = await f1.merge(with: f2, bufferingPolicy: .bufferingNewest(64)).collect()
    #expect(Set(res) == Set([1, 2, 3, 4]))
}

@available(*, deprecated, message: "Exercises deprecated compatibility overloads.")
@Test func mergeDeprecatedOverloads() async {
    let f1 = AsyncRay.from([1, 2])
    let f2 = AsyncRay.from([3, 4])

    let res1 = await merge(f1, f2).collect()
    #expect(Set(res1) == Set([1, 2, 3, 4]))

    let res2 = await merge([f1, f2]).collect()
    #expect(Set(res2) == Set([1, 2, 3, 4]))

    let res3 = await f1.merge(with: f2).collect()
    #expect(Set(res3) == Set([1, 2, 3, 4]))
}

@Test func mergeWithNativeAsyncStream() async {
    let asyncRay = AsyncRay.from([1, 2])
    let nativeStream = AsyncStream<Int> { continuation in
        Task {
            try? await Task.sleep(for: .milliseconds(10))
            continuation.yield(3)
            continuation.yield(4)
            continuation.finish()
        }
    }

    let merged = await asyncRay.merge(with: nativeStream, bufferingPolicy: .bufferingNewest(64))
        .collect()
    #expect(Set(merged) == Set([1, 2, 3, 4]))
}
