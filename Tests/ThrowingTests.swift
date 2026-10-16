import Testing
@testable import AsyncRay

private struct TestError: Error, Equatable {
    let code: Int
}

/// A cold source that fails on its first `failures` subscriptions and then emits `[1, 2]`.
private func flaky(failures: Int, attempts: Collector<Int>) -> AsyncThrowingRay<Int> {
    AsyncThrowingRay { emitter in
        attempts.append(1)
        if attempts.count() <= failures {
            emitter.send(0)
            emitter.fail(TestError(code: attempts.count()))
        } else {
            emitter.send(1)
            emitter.send(2)
            emitter.finish()
        }
    }
}

@Suite(.timeLimit(.minutes(1)))
struct ThrowingTests {

    // MARK: - Creation and subscription

    @Test func fromOperationEmitsResultAndCompletes() async throws {
        let values = try await AsyncThrowingRay.from { 42 }.collect()
        #expect(values == [42])
    }

    @Test func fromOperationFailureReachesFailureCallbackOnly() async {
        let failures = Collector<TestError>()
        let completed = Collector<Bool>()
        let sub = AsyncThrowingRay<Int>.from { throw TestError(code: 7) }
            .sink(
                next: { _ in },
                failure: { failures.append($0 as! TestError) },
                completed: { completed.append(true) }
            )
        defer { sub.cancel() }

        #expect(await eventually { failures.values == [TestError(code: 7)] })
        #expect(completed.values.isEmpty)
    }

    @Test func emitterDeliversValuesBeforeFailure() async {
        let values = Collector<Int>()
        let failures = Collector<TestError>()
        let sub = AsyncThrowingRay<Int> { emitter in
            emitter.send(1)
            emitter.send(2)
            emitter.fail(TestError(code: 1))
            emitter.send(3)  // Ignored after failure
        }.sink(next: { values.append($0) }, failure: { failures.append($0 as! TestError) })
        defer { sub.cancel() }

        #expect(await eventually { failures.values == [TestError(code: 1)] })
        #expect(values.values == [1, 2])
    }

    @Test func emitterReportsFailureAsTerminationReason() async {
        let reasons = Collector<String>()
        let sub = AsyncThrowingRay<Int> { emitter in
            emitter.onTermination { reason in
                switch reason {
                case .finished(let error?): reasons.append("failed \((error as! TestError).code)")
                case .finished(nil): reasons.append("finished")
                case .cancelled: reasons.append("cancelled")
                @unknown default: reasons.append("unknown")
                }
            }
            emitter.fail(TestError(code: 3))
        }.sink(next: { _ in }, failure: { _ in })
        defer { sub.cancel() }

        #expect(await eventually { reasons.values == ["failed 3"] })
    }

    @Test func collectAndFirstRethrow() async {
        let failing = AsyncThrowingRay<Int>.fail(TestError(code: 5))
        await #expect(throws: TestError(code: 5)) { try await failing.collect() }
        await #expect(throws: TestError(code: 5)) { try await failing.first() }
    }

    @Test func cancellationReportsNeitherFailureNorCompletion() async {
        let started = Collector<Bool>()
        let cancelled = Collector<Bool>()
        let callbacks = Collector<String>()
        let sub = AsyncThrowingRay<Int>.from {
            started.append(true)
            do {
                try await Task.sleep(for: .seconds(60))
            } catch {
                cancelled.append(true)
                throw error
            }
            return 1
        }.sink(
            next: { _ in callbacks.append("next") },
            failure: { _ in callbacks.append("failure") },
            completed: { callbacks.append("completed") }
        )
        #expect(await eventually { started.values == [true] })

        sub.cancel()
        #expect(await eventually { cancelled.values == [true] })
        try? await Task.sleep(for: .milliseconds(20))
        #expect(callbacks.values.isEmpty)
    }

    @Test func failedSubscriptionIsPrunedFromBag() async {
        let bag = SubscriptionBag()
        AsyncThrowingRay<Int>.fail(TestError(code: 1))
            .sink(next: { _ in }, failure: { _ in })
            .store(in: bag)
        #expect(await eventually { bag.count == 0 })
    }

    // MARK: - Operators

    @Test func operatorsForwardUpstreamFailure() async {
        let source = AsyncThrowingRay<Int> { emitter in
            emitter.send(1)
            emitter.send(2)
            emitter.send(3)
            emitter.fail(TestError(code: 9))
        }
        let chained = source.map { $0 * 10 }.filter { $0 != 20 }.compactMap { $0 }
        let results = await chained.asResult().collect()
        #expect(results.compactMap { try? $0.get() } == [10, 30])
        #expect(results.count == 3)
    }

    @Test func tryMapFailsAndCancelsUpstream() async {
        let pipe = Pipe<String>()
        let failures = Collector<Error>()
        let values = Collector<Int>()
        let sub = pipe.asyncRay
            .tryMap { text -> Int in
                guard let number = Int(text) else { throw TestError(code: 0) }
                return number
            }
            .sink(next: { values.append($0) }, failure: { failures.append($0) })
        defer { sub.cancel() }
        #expect(await eventually { pipe.subscriberCount == 1 })

        pipe.send("1")
        pipe.send("x")
        pipe.send("3")
        #expect(await eventually { failures.count() == 1 })
        #expect(values.values == [1])
        #expect(await eventually { pipe.subscriberCount == 0 })
    }

    @Test func asyncMapRunsThrowingWorkSequentially() async throws {
        let values = try await AsyncThrowingRay.from([1, 2, 3])
            .asyncMap { value -> Int in
                await Task.yield()
                return value * 2
            }
            .collect()
        #expect(values == [2, 4, 6])
    }

    @Test func onFailureObservesErrorAndForwardsIt() async {
        let observed = Collector<Int>()
        let result = await AsyncThrowingRay<Int>.fail(TestError(code: 4))
            .onFailure { observed.append(($0 as! TestError).code) }
            .asResult()
            .collect()
        #expect(observed.values == [4])
        #expect(result.count == 1)
    }

    // MARK: - Error handling

    @Test func catchKeepsEarlierValuesAndSwitchesToFallback() async {
        let values = await AsyncThrowingRay<Int> { emitter in
            emitter.send(1)
            emitter.fail(TestError(code: 1))
        }
        .catch { _ in AsyncRay.from([8, 9]) }
        .collect()
        #expect(values == [1, 8, 9])
    }

    @Test func catchIsNotUsedWhenUpstreamCompletes() async {
        let fallbackUsed = Collector<Bool>()
        let values = await AsyncThrowingRay.from([1, 2])
            .catch { _ in
                fallbackUsed.append(true)
                return .just(0)
            }
            .collect()
        #expect(values == [1, 2])
        #expect(fallbackUsed.values.isEmpty)
    }

    @Test func replaceErrorEmitsValueAndCompletes() async {
        let values = await AsyncThrowingRay<Int>.fail(TestError(code: 1))
            .replaceError(with: -1)
            .collect()
        #expect(values == [-1])
    }

    @Test func asResultMaterializesValuesAndFailure() async throws {
        let results = await AsyncThrowingRay<Int> { emitter in
            emitter.send(1)
            emitter.fail(TestError(code: 2))
        }.asResult().collect()

        #expect(results.count == 2)
        #expect(try results[0].get() == 1)
        #expect(throws: TestError(code: 2)) { try results[1].get() }
    }

    @Test func retryResubscribesUntilSuccess() async throws {
        let attempts = Collector<Int>()
        let values = try await flaky(failures: 2, attempts: attempts).retry(2).collect()
        #expect(attempts.count() == 3)
        // Values from failed attempts are already delivered.
        #expect(values == [0, 0, 1, 2])
    }

    @Test func retryFailsWithLastErrorWhenExhausted() async {
        let attempts = Collector<Int>()
        await #expect(throws: TestError(code: 3)) {
            try await flaky(failures: 5, attempts: attempts).retry(2).collect()
        }
        #expect(attempts.count() == 3)
    }

    @Test func retryZeroDoesNotResubscribe() async {
        let attempts = Collector<Int>()
        await #expect(throws: TestError(code: 1)) {
            try await flaky(failures: 1, attempts: attempts).retry(0).collect()
        }
        #expect(attempts.count() == 1)
    }

    @Test func retryWaitsForDelayOnClock() async {
        let clock = ManualClock()
        let attempts = Collector<Int>()
        let completed = Collector<Bool>()
        let sub = flaky(failures: 1, attempts: attempts)
            .retry(1, delay: .seconds(5), clock: clock)
            .sink(next: { _ in }, failure: { _ in }, completed: { completed.append(true) })
        defer { sub.cancel() }

        #expect(await eventually { clock.sleepCount == 1 })
        clock.advance(by: .seconds(4))
        #expect(attempts.count() == 1)

        clock.advance(by: .seconds(1))
        #expect(await eventually { completed.values == [true] })
        #expect(attempts.count() == 2)
    }

    @Test func asThrowingNeverFails() async throws {
        let values = try await AsyncRay.from([1, 2, 3]).asThrowing().collect()
        #expect(values == [1, 2, 3])
    }

    @Test func throwingRayIsAnAsyncSequence() async throws {
        var sum = 0
        for try await value in AsyncThrowingRay.from([1, 2, 3]) {
            sum += value
        }
        #expect(sum == 6)
    }
}
