import Foundation
import Testing
@testable import AsyncRay

private final class DeliveryLifetime: Sendable {
    let ended: AsyncStream<Void>.Continuation

    init(_ ended: AsyncStream<Void>.Continuation) {
        self.ended = ended
    }

    deinit {
        ended.yield(())
        ended.finish()
    }
}

@MainActor private final class MainSubscriptionSlot {
    var subscription: Subscription?
}

@Suite(.timeLimit(.minutes(1)))
struct FoundationRegressionTests {
    @Test func storageModifyAndDistinctAreSingleTransactions() async {
        let storage = _CurrentValueStorage(0)
        let output = AsyncStream<Int>.makeStream()
        storage.registerAndReplay(id: UUID(), continuation: output.continuation)
        storage.modify { $0 + 1 }
        storage.modify { $0 + 1 }
        storage.setDistinct(2)
        storage.modifyDistinct { $0 }
        storage.modifyDistinct { $0 + 1 }
        #expect(storage.value == 3)
        output.continuation.finish()
        #expect(await AsyncRay { output.stream }.collect() == [0, 1, 2, 3])
    }

    @Test func registrationReplaysBeforeTheNextWrite() async {
        let storage = _CurrentValueStorage(0)
        let output = AsyncStream<Int>.makeStream()
        storage.registerAndReplay(id: UUID(), continuation: output.continuation)
        storage.set(1)
        output.continuation.finish()
        #expect(await AsyncRay { output.stream }.collect() == [0, 1])
    }

    @Test func latestBufferCannotBeReplacedByStaleReplay() async {
        let storage = _CurrentValueStorage(0)
        let output = AsyncStream<Int>.makeStream(bufferingPolicy: .bufferingNewest(1))
        storage.registerAndReplay(id: UUID(), continuation: output.continuation)
        storage.set(1)
        output.continuation.finish()
        #expect(await AsyncRay { output.stream }.collect() == [1])
    }

    @Test func cancelledRegistrationCannotLeakAContinuation() {
        let storage = _CurrentValueStorage(0)
        let output = AsyncStream<Int>.makeStream()
        let id = UUID()
        // Cleanup may reach the actor before the asynchronous registration does.
        output.continuation.finish()
        storage.removeContinuation(id: id)
        storage.registerAndReplay(id: id, continuation: output.continuation)
        #expect(storage.subscriberCount == 0)
    }

    @Test func publicModifyDoesNotLoseConcurrentWrites() async {
        let state = CurrentValue(0)
        let distinct = CurrentValueDistinct(0)
        // Integration coverage supplements the deterministic storage transactions.
        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<100 {
                group.addTask {
                    await state.modify { $0 + 1 }
                    await distinct.modify { $0 + 1 }
                }
            }
        }
        #expect(await state.value == 100)
        #expect(await distinct.value == 100)
    }

    @Test func releasingStateFinishesExistingSubscribers() async {
        var state: CurrentValue<Int>? = CurrentValue(7)
        let isReleased = { [weak state] in state == nil }
        let stream = state!.stream
        var iterator = stream.makeAsyncIterator()
        #expect(await iterator.next() == 7)
        state = nil
        #expect(await iterator.next() == nil)
        #expect(isReleased())
    }

    @Test @MainActor func cancelBeforeMainDeliveryDiscardsBufferedValues() async {
        let values = Collector<Int>()
        let ended = AsyncStream<Void>.makeStream()
        let subscription = cancelledMainSubscription(values: values, ended: ended.continuation)
        for await _ in ended.stream {}
        #expect(values.values.isEmpty)
        withExtendedLifetime(subscription) {}
    }

    @MainActor private func cancelledMainSubscription(
        values: Collector<Int>,
        ended: AsyncStream<Void>.Continuation
    ) -> Subscription {
        let lifetime = DeliveryLifetime(ended)
        let subscription = AsyncRay.from([1, 2, 3]).sinkOnMain { [lifetime] value in
            values.append(value)
            withExtendedLifetime(lifetime) {}
        }
        // No actor suspension: the sink task cannot have entered its loop yet.
        subscription.cancel()
        return subscription
    }

    @Test @MainActor func cancelInMainCallbackStopsRemainingBuffer() async {
        let values = Collector<Int>()
        let ended = AsyncStream<Void>.makeStream()
        let slot = MainSubscriptionSlot()
        installSelfCancellingMainSink(slot: slot, values: values, ended: ended.continuation)
        for await _ in ended.stream {}
        #expect(values.values == [1])
        slot.subscription = nil
    }

    @MainActor private func installSelfCancellingMainSink(
        slot: MainSubscriptionSlot,
        values: Collector<Int>,
        ended: AsyncStream<Void>.Continuation
    ) {
        let lifetime = DeliveryLifetime(ended)
        slot.subscription = AsyncRay.from([1, 2, 3]).sinkOnMain { [lifetime] value in
            values.append(value)
            slot.subscription?.cancel()
            withExtendedLifetime(lifetime) {}
        }
    }

    @Test(arguments: [false, true])
    func cancelDuringBackgroundDeliveryStopsBuffer(withCompletion: Bool) async {
        let entered = AsyncStream<Void>.makeStream()
        let ended = AsyncStream<Void>.makeStream()
        let resume = DispatchSemaphore(value: 0)
        let values = Collector<Int>()
        let completed = Collector<Bool>()
        let subscription = blockedSink(
            values: values, completed: completed, entered: entered.continuation,
            ended: ended.continuation, resume: resume, withCompletion: withCompletion
        )
        var arrivals = entered.stream.makeAsyncIterator()
        #expect(await arrivals.next() != nil)
        subscription.cancel()
        resume.signal()
        for await _ in ended.stream {}
        #expect(values.values == [1])
        #expect(completed.values.isEmpty)
        withExtendedLifetime(subscription) {}
    }

    private func blockedSink(
        values: Collector<Int>, completed: Collector<Bool>,
        entered: AsyncStream<Void>.Continuation, ended: AsyncStream<Void>.Continuation,
        resume: DispatchSemaphore, withCompletion: Bool
    ) -> Subscription {
        let lifetime = DeliveryLifetime(ended)
        let handler: @Sendable (Int) -> Void = { [lifetime] value in
            values.append(value)
            if value == 1 {
                entered.yield(())
                #expect(resume.wait(timeout: .now() + 5) == .success)
            }
            withExtendedLifetime(lifetime) {}
        }
        if withCompletion {
            return AsyncRay.from([1, 2, 3]).sink(next: handler, completed: { completed.append(true) })
        }
        return AsyncRay.from([1, 2, 3]).sink(handler)
    }

    @Test func oldResponseResumingAfterSwitchCannotYield() async throws {
        let output = AsyncStream<Int>.makeStream()
        let latest = _LatestSubscription(continuation: output.continuation)
        let old = try #require(await latest.replace(with: .never()))
        await latest.yield(1, generation: old)
        let current = try #require(await latest.replace(with: .never()))
        // The old producer passed its own cancellation check before the switch;
        // resume exactly its downstream delivery operation after replacement.
        await latest.yield(10, generation: old)
        await latest.yield(20, generation: current)
        await latest.finish()
        #expect(await AsyncRay { output.stream }.collect() == [1, 20])
    }

    @Test func finishPreventsLateInstallationAndDelivery() async throws {
        let output = AsyncStream<Int>.makeStream()
        let latest = _LatestSubscription(continuation: output.continuation)
        let generation = try #require(await latest.replace(with: .never()))
        await latest.finish()
        #expect(await latest.replace(with: .just(30)) == nil)
        await latest.yield(10, generation: generation)
        await latest.finish()
        #expect(await AsyncRay { output.stream }.collect().isEmpty)
    }

    @Test func latestOperatorCancelsOldSourceAndRejectsItsLateResponse() async {
        let outer = AsyncStream<Int>.makeStream()
        let old = AsyncStream<Int>.makeStream(bufferingPolicy: .bufferingNewest(1))
        let fresh = AsyncStream<Int>.makeStream(bufferingPolicy: .bufferingNewest(1))
        let registered = AsyncStream<Int>.makeStream()
        let oldEnded = AsyncStream<Void>.makeStream()
        old.continuation.onTermination = { _ in
            oldEnded.continuation.yield(())
            oldEnded.continuation.finish()
        }
        var registrations = registered.stream.makeAsyncIterator()
        var iterator = AsyncRay { outer.stream }
            .flatMapLatest(bufferingPolicy: .bufferingNewest(1)) { id in
                AsyncRay {
                    registered.continuation.yield(id)
                    return id == 1 ? old.stream : fresh.stream
                }
            }.stream.makeAsyncIterator()
        outer.continuation.yield(1)
        #expect(await registrations.next() == 1)
        outer.continuation.yield(2)
        #expect(await registrations.next() == 2)
        for await _ in oldEnded.stream {}
        if case .terminated = old.continuation.yield(10) {} else {
            Issue.record("Cancelled source accepted a late response")
        }
        fresh.continuation.yield(20)
        #expect(await iterator.next() == 20)
        // Outer completion keeps the latest inner alive until it completes itself.
        outer.continuation.finish()
        fresh.continuation.yield(21)
        #expect(await iterator.next() == 21)
        fresh.continuation.finish()
        #expect(await iterator.next() == nil)
        registered.continuation.finish()
    }

    @Test func outerCompletionWaitsForLatestInner() async throws {
        let output = AsyncStream<Int>.makeStream()
        let latest = _LatestSubscription(continuation: output.continuation)
        let old = try #require(await latest.replace(with: .never()))
        let current = try #require(await latest.replace(with: .never()))
        await latest.outerCompleted()
        await latest.yield(1, generation: current)
        // A stale inner completing must neither finish output nor admit its values.
        await latest.innerCompleted(generation: old)
        await latest.yield(10, generation: old)
        await latest.yield(2, generation: current)
        await latest.innerCompleted(generation: current)
        await latest.yield(3, generation: current)
        #expect(await AsyncRay { output.stream }.collect() == [1, 2])
    }

    @Test func outerCompletionWithoutActiveInnerFinishesImmediately() async throws {
        let output = AsyncStream<Int>.makeStream()
        let latest = _LatestSubscription(continuation: output.continuation)
        let generation = try #require(await latest.replace(with: .never()))
        await latest.yield(1, generation: generation)
        await latest.innerCompleted(generation: generation)
        await latest.outerCompleted()
        #expect(await latest.replace(with: .just(2)) == nil)
        #expect(await AsyncRay { output.stream }.collect() == [1])
    }

    @Test func finiteOuterDeliversLatestInnerResult() async {
        // Regression: outer completion used to cancel the inner before it could emit,
        // so `AsyncRay.from(...).flatMapLatest { request }` produced nothing.
        let values = await AsyncRay.from([1, 2, 3])
            .flatMapLatest(bufferingPolicy: .unbounded) { n in
                AsyncRay<Int> { emitter in
                    Task {
                        try? await Task.sleep(for: .milliseconds(20))
                        emitter.send(n * 10)
                        emitter.finish()
                    }
                }
            }
            .collect()
        #expect(values == [30])
    }

    @Test func combineLatestLastEmissionIsLatestState() async throws {
        // Regression: tuples were yielded outside the state lock, so a stale tuple
        // built earlier by one side could be delivered after a newer one.
        for _ in 0..<20 {
            let values = await combineLatest(
                AsyncRay.from(0..<2_000),
                AsyncRay.from(0..<2_000),
                bufferingPolicy: .unbounded
            ).collect()
            let last = try #require(values.last)
            #expect(last.0 == 1_999 && last.1 == 1_999)
        }
    }

    @Test func delayShiftsTimelineInsteadOfSerializing() async {
        let clock = ContinuousClock()
        let start = clock.now
        let values = await AsyncRay.from([1, 2, 3, 4, 5]).delay(.milliseconds(100)).collect()
        let elapsed = clock.now - start
        #expect(values == [1, 2, 3, 4, 5])
        #expect(elapsed >= .milliseconds(100))
        // A serialized delay needs at least 5 × 100 ms.
        #expect(elapsed < .milliseconds(400))
    }

    @Test func onceValueThrowsWhenCallerIsCancelled() async throws {
        let once = Once<Int>()
        let waiter = Task { try await once.value }
        try? await Task.sleep(for: .milliseconds(20))
        waiter.cancel()
        let result = await waiter.result
        #expect(throws: CancellationError.self) { try result.get() }
        // A cancelled waiter must not block or consume a later resolution.
        await once.resolve(5)
        #expect(try await once.value == 5)
    }
}
