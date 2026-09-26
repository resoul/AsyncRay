import Testing
@testable import AsyncRay

// MARK: - map

@Test func mapTransformsValues() async {
    let values = await AsyncRay.from([1, 2, 3]).map { $0 * 10 }.collect()
    #expect(values == [10, 20, 30])
}

@Test func asyncMapTransformsValues() async {
    let values = await AsyncRay.from([1, 2, 3]).asyncMap { val in
        try? await Task.sleep(for: .milliseconds(10))
        return "num-\(val)"
    }.collect()
    #expect(values == ["num-1", "num-2", "num-3"])
}

@Test func asyncMapCancellation() async {
    let pipe = Pipe<Int>()
    let results = Collector<String>()

    let sub = pipe.asyncRay.asyncMap { val -> String in
        try? await Task.sleep(for: .milliseconds(50))
        return "async-\(val)"
    }.sink { v in
        results.append(v)
    }

    try? await Task.sleep(for: .milliseconds(10))
    pipe.send(1)
    try? await Task.sleep(for: .milliseconds(10))
    sub.cancel()
    try? await Task.sleep(for: .milliseconds(100))

    let all = results.values
    #expect(all.isEmpty)
}

@Test func compactMapFiltersNils() async {
    let values = await AsyncRay.from(["1", "x", "3"]).compactMap { Int($0) }.collect()
    #expect(values == [1, 3])
}

// MARK: - filter

@Test func filterKeepsMatchingValues() async {
    let values = await AsyncRay.from([1, 2, 3, 4, 5]).filter { $0 % 2 == 0 }.collect()
    #expect(values == [2, 4])
}

@Test func filterNoMatches() async {
    let values = await AsyncRay.from([1, 3, 5]).filter { $0 % 2 == 0 }.collect()
    #expect(values.isEmpty)
}

// MARK: - take / skip

@Test func takeFirstN() async {
    let values = await AsyncRay.from([1, 2, 3, 4, 5]).take(3).collect()
    #expect(values == [1, 2, 3])
}

@Test func takeZero() async {
    let values = await AsyncRay.from([1, 2, 3]).take(0).collect()
    #expect(values.isEmpty)
}

@Test func takeNegative() async {
    let values = await AsyncRay.from([1, 2, 3]).take(-5).collect()
    #expect(values.isEmpty)
}

@Test func takeMoreThanAvailable() async {
    let values = await AsyncRay.from([1, 2]).take(10).collect()
    #expect(values == [1, 2])
}

@Test func skipFirstN() async {
    let values = await AsyncRay.from([1, 2, 3, 4, 5]).skip(2).collect()
    #expect(values == [3, 4, 5])
}

@Test func skipZero() async {
    let values = await AsyncRay.from([1, 2, 3]).skip(0).collect()
    #expect(values == [1, 2, 3])
}

@Test func skipNegative() async {
    let values = await AsyncRay.from([1, 2, 3]).skip(-2).collect()
    #expect(values == [1, 2, 3])
}

@Test func skipMoreThanAvailable() async {
    let values = await AsyncRay.from([1, 2]).skip(5).collect()
    #expect(values.isEmpty)
}

// MARK: - prefix(while:) / drop(while:)

@Test func prefixWhile() async {
    let values = await AsyncRay.from([1, 2, 3, 4, 1, 2]).prefix(while: { $0 < 4 }).collect()
    #expect(values == [1, 2, 3])
}

@Test func prefixWhileImmediateFalse() async {
    let values = await AsyncRay.from([5, 1, 2]).prefix(while: { $0 < 4 }).collect()
    #expect(values.isEmpty)
}

@Test func dropWhile() async {
    let values = await AsyncRay.from([1, 2, 3, 4, 1, 2]).drop(while: { $0 < 3 }).collect()
    #expect(values == [3, 4, 1, 2])
}

@Test func dropWhileAllMatch() async {
    let values = await AsyncRay.from([1, 2, 3]).drop(while: { $0 < 10 }).collect()
    #expect(values.isEmpty)
}

// MARK: - skipRepeats

@Test func skipRepeatsDeduplicates() async {
    let values = await AsyncRay.from([1, 1, 2, 2, 3, 1]).skipRepeats().collect()
    #expect(values == [1, 2, 3, 1])
}

@Test func skipRepeatsAllSame() async {
    let values = await AsyncRay.from([5, 5, 5]).skipRepeats().collect()
    #expect(values == [5])
}

@Test func skipRepeatsEmpty() async {
    let values = await AsyncRay<Int>.empty().skipRepeats().collect()
    #expect(values.isEmpty)
}

// MARK: - flatMap

@available(*, deprecated, message: "Exercises deprecated compatibility overloads.")
@Test func flatMapDeprecatedUnbounded() async {
    let values = await AsyncRay.from([1, 2])
        .flatMap { n in AsyncRay.from([n, n * 10]) }
        .collect()
    #expect(Set(values) == Set([1, 10, 2, 20]))
}

// MARK: - flatMapLatest

@Test func flatMapLatestCancelsPrevious() async {
    let pipe = Pipe<Int>()
    let results = Collector<String>()

    let sub = pipe.asyncRay
        .flatMapLatest(bufferingPolicy: .unbounded) { n -> AsyncRay<String> in
            AsyncRay<String> { emitter in
                Task {
                    try? await Task.sleep(for: .milliseconds(50))
                    guard !Task.isCancelled else { return }
                    emitter.send("result-\(n)")
                    emitter.finish()
                }
            }
        }
        .sink { value in results.append(value) }

    try? await Task.sleep(for: .milliseconds(10))
    pipe.send(1)
    pipe.send(2)  // cancels inner for 1
    pipe.send(3)  // cancels inner for 2

    try? await Task.sleep(for: .milliseconds(200))
    sub.cancel()

    let all = results.values
    #expect(all == ["result-3"])
}

@available(*, deprecated, message: "Exercises deprecated compatibility overloads.")
@Test func flatMapLatestDeprecatedUnbounded() async {
    let values = await AsyncRay.just(1).flatMapLatest { AsyncRay.from([$0, $0 + 1]) }.collect()
    #expect(values == [1, 2])
}

@Test func flatMapLatestOnJustEmitsInnerValue() async {
    let values = await AsyncRay.just(1)
        .flatMapLatest(bufferingPolicy: .unbounded) { AsyncRay.from([$0, $0 + 1]) }
        .collect()
    #expect(values == [1, 2])
}

// MARK: - delay

@Test func delayDropsInFlightValuesOnCancel() async {
    let received = Collector<Int>()
    let sub = AsyncRay.from([1, 2]).delay(.milliseconds(100)).sink { received.append($0) }
    try? await Task.sleep(for: .milliseconds(20))
    sub.cancel()
    try? await Task.sleep(for: .milliseconds(150))
    #expect(received.values.isEmpty)
}

// MARK: - then

@available(*, deprecated, message: "Exercises deprecated compatibility overloads.")
@Test func thenDeprecatedUnbounded() async {
    let values = await AsyncRay.from([1, 2]).then(AsyncRay.from([3, 4])).collect()
    #expect(values == [1, 2, 3, 4])
}

@Test func thenChainsAsyncRayes() async {
    let values = await AsyncRay.from([1, 2]).then(
        AsyncRay.from([3, 4]),
        bufferingPolicy: .unbounded
    ).collect()
    #expect(values == [1, 2, 3, 4])
}

// MARK: - merge

@Test func mergeCombinesStreams() async {
    let pipe1 = Pipe<Int>()
    let pipe2 = Pipe<Int>()
    let collector = Pipe<Int>()
    let results = Collector<Int>()
    let colSub = collector.asyncRay.sink { v in results.append(v) }

    let sub = merge([pipe1.asyncRay, pipe2.asyncRay], bufferingPolicy: .bufferingNewest(64))
        .sink { v in collector.send(v) }

    try? await Task.sleep(for: .milliseconds(10))
    pipe1.send(1)
    pipe2.send(2)
    pipe1.send(3)

    try? await Task.sleep(for: .milliseconds(50))
    sub.cancel()
    colSub.cancel()

    let all = results.values
    #expect(all.count == 3)
    #expect(Set(all) == Set([1, 2, 3]))
}

// MARK: - combineLatest

@Test func combineLatestWaitsForBoth() async {
    let p1 = Pipe<Int>()
    let p2 = Pipe<String>()
    let resultPipe = Pipe<(Int, String)>()
    let results = Collector<(Int, String)>()
    let colSub = resultPipe.asyncRay.sink { pair in results.append(pair) }

    let sub = combineLatest(p1.asyncRay, p2.asyncRay, bufferingPolicy: .bufferingNewest(64))
        .sink { pair in resultPipe.send(pair) }

    try? await Task.sleep(for: .milliseconds(10))
    p1.send(1)
    try? await Task.sleep(for: .milliseconds(30))
    let beforeBoth = results.values
    #expect(beforeBoth.isEmpty)

    p2.send("a")
    try? await Task.sleep(for: .milliseconds(50))
    let afterBoth = results.values
    #expect(afterBoth.count == 1)
    #expect(afterBoth[0].0 == 1)
    #expect(afterBoth[0].1 == "a")

    p1.send(2)
    try? await Task.sleep(for: .milliseconds(50))
    let afterUpdate = results.values
    #expect(afterUpdate.count == 2)
    #expect(afterUpdate[1].0 == 2)
    #expect(afterUpdate[1].1 == "a")

    sub.cancel()
    colSub.cancel()
}

// MARK: - debounce

@Test func debounceDelaysAndTakesLast() async {
    let pipe = Pipe<Int>()
    let results = Collector<Int>()

    let sub = pipe.asyncRay
        .debounce(.milliseconds(50))
        .sink { v in results.append(v) }

    try? await Task.sleep(for: .milliseconds(10))
    pipe.send(1)
    pipe.send(2)
    pipe.send(3)

    try? await Task.sleep(for: .milliseconds(150))
    sub.cancel()

    let all = results.values
    #expect(all == [3])
}

// MARK: - throttle

@Test func throttleLimitsRate() async {
    let pipe = Pipe<Int>()
    let results = Collector<Int>()

    let sub = pipe.asyncRay
        .throttle(.milliseconds(100))
        .sink { v in results.append(v) }

    try? await Task.sleep(for: .milliseconds(10))
    pipe.send(1)
    pipe.send(2)  // too fast — skipped
    pipe.send(3)  // too fast — skipped

    try? await Task.sleep(for: .milliseconds(150))
    pipe.send(4)  // interval elapsed — emitted

    try? await Task.sleep(for: .milliseconds(50))
    sub.cancel()

    let all = results.values
    #expect(all.contains(1))
    #expect(all.contains(4))
    #expect(!all.contains(2))
    #expect(!all.contains(3))
}

// MARK: - AsyncStream bridge

@Test func asyncStreamConvertedToAsyncRay() async {
    let asyncStream = AsyncStream<Int> { continuation in
        continuation.yield(10)
        continuation.yield(20)
        continuation.finish()
    }

    let results = Collector<Int>()
    let sub = asyncStream.asAsyncRay().sink { v in results.append(v) }
    try? await Task.sleep(for: .milliseconds(30))
    sub.cancel()

    let all = results.values
    #expect(all.contains(10))
    #expect(all.contains(20))
}

// MARK: - Systematic Buffering Policy Propagation

@Test func policyPropagationAcrossAllUnaryOperators() {
    let base = AsyncRay<Int>(bufferingPolicy: .bufferingNewest(1)) {
        AsyncStream<Int>(bufferingPolicy: .bufferingNewest(1)) { $0.finish() }
    }

    #expect(base.inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.map { $0 * 2 }.inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.asyncMap { $0 * 2 }.inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.compactMap { $0 }.inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.filter { $0 > 0 }.inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.take(2).inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.skip(1).inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.prefix(while: { $0 > 0 }).inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.drop(while: { $0 < 0 }).inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.skipRepeats().inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.delay(.milliseconds(10)).inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.debounce(.milliseconds(10)).inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.throttle(.milliseconds(10)).inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.timeout(.milliseconds(100)).inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.onMain().inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.onBackground().inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.onCompletion {}.inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.scan(0) { $0 + $1 }.inheritedBufferingPolicy == .bufferingNewest(1))
    #expect(base.reduce(0) { $0 + $1 }.inheritedBufferingPolicy == .bufferingNewest(1))
}

@Test func policyPropagationAcrossFanInOperators() {
    let f1 = AsyncRay<Int>.just(1)
    let f2 = AsyncRay<String>.just("a")
    let f3 = AsyncRay<Bool>.just(true)
    let f4 = AsyncRay<Double>.just(1.0)

    let m = merge([f1, f1], bufferingPolicy: .bufferingNewest(16))
    #expect(m.inheritedBufferingPolicy == .bufferingNewest(16))
    #expect(m.map { $0 * 2 }.inheritedBufferingPolicy == .bufferingNewest(16))

    let c2 = combineLatest(f1, f2, bufferingPolicy: .bufferingNewest(32))
    #expect(c2.inheritedBufferingPolicy == .bufferingNewest(32))
    #expect(c2.map { "\($0.0)-\($0.1)" }.inheritedBufferingPolicy == .bufferingNewest(32))

    let c3 = combineLatest(f1, f2, f3, bufferingPolicy: .bufferingNewest(8))
    #expect(c3.inheritedBufferingPolicy == .bufferingNewest(8))

    let c4 = combineLatest(f1, f2, f3, f4, bufferingPolicy: .bufferingNewest(4))
    #expect(c4.inheritedBufferingPolicy == .bufferingNewest(4))

    let z = zip(f1, f2, bufferingPolicy: .bufferingNewest(64))
    #expect(z.inheritedBufferingPolicy == .bufferingNewest(64))
    #expect(z.filter { _ in true }.inheritedBufferingPolicy == .bufferingNewest(64))

    let fm = f1.flatMap(maxConcurrent: 2, bufferingPolicy: .bufferingNewest(12)) {
        AsyncRay.just($0)
    }
    #expect(fm.inheritedBufferingPolicy == .bufferingNewest(12))

    let fml = f1.flatMapLatest(bufferingPolicy: .bufferingNewest(24)) { AsyncRay.just($0) }
    #expect(fml.inheritedBufferingPolicy == .bufferingNewest(24))

    let t = f1.then(f1, bufferingPolicy: .bufferingNewest(48))
    #expect(t.inheritedBufferingPolicy == .bufferingNewest(48))
}
