import Testing
@testable import AsyncRay

// Timing operators driven by ManualClock: time moves only when the test advances it,
// so these tests assert exact deadlines instead of relying on wall-clock sleeps.

@Suite(.timeLimit(.minutes(1)))
struct ClockTests {

    @Test func debounceEmitsLatestOnlyAfterQuietWindow() async {
        let clock = ManualClock()
        let pipe = Pipe<Int>()
        let values = Collector<Int>()
        let sub = pipe.asyncRay.debounce(.seconds(1), clock: clock).sink { values.append($0) }
        defer { sub.cancel() }
        #expect(await eventually { pipe.subscriberCount == 1 })

        pipe.send(1)
        #expect(await eventually { clock.sleepCount == 1 })
        clock.advance(by: .milliseconds(500))
        pipe.send(2)
        #expect(await eventually { clock.sleepCount == 2 })

        // The window restarted at 0.5s, so 1s total is not yet quiet long enough.
        clock.advance(by: .milliseconds(500))
        #expect(clock.pendingSleepers == 1)
        #expect(values.values.isEmpty)

        clock.advance(by: .milliseconds(500))
        #expect(await eventually { values.values == [2] })
    }

    @Test func throttleDropsValuesInsideWindow() async {
        let clock = ManualClock()
        let values = await AsyncRay.from([1, 2, 3])
            .throttle(.seconds(1), clock: clock)
            .collect()
        #expect(values == [1])
    }

    @Test func throttleForwardsAfterWindowElapses() async {
        let clock = ManualClock()
        let pipe = Pipe<Int>()
        let values = Collector<Int>()
        let sub = pipe.asyncRay.throttle(.seconds(1), clock: clock).sink { values.append($0) }
        defer { sub.cancel() }
        #expect(await eventually { pipe.subscriberCount == 1 })

        pipe.send(1)
        #expect(await eventually { values.values == [1] })
        clock.advance(by: .seconds(1))
        pipe.send(2)
        #expect(await eventually { values.values == [1, 2] })
    }

    @Test func delayHoldsValuesUntilDeadline() async {
        let clock = ManualClock()
        let values = Collector<Int>()
        let completed = Collector<Bool>()
        let sub = AsyncRay.from([1, 2, 3])
            .delay(.seconds(1), clock: clock)
            .sink(next: { values.append($0) }, completed: { completed.append(true) })
        defer { sub.cancel() }
        // delay reads `now` once per value to stamp its deadline; wait until all three are stamped.
        #expect(await eventually { clock.nowReadCount == 3 && clock.sleepCount == 1 })

        clock.advance(by: .milliseconds(999))
        #expect(clock.pendingSleepers == 1)
        #expect(values.values.isEmpty)

        clock.advance(by: .milliseconds(1))
        #expect(await eventually { completed.values == [true] })
        #expect(values.values == [1, 2, 3])
    }

    @Test func timeoutCompletesAfterQuietPeriodAndResetsOnValue() async {
        let clock = ManualClock()
        let pipe = Pipe<Int>()
        let values = Collector<Int>()
        let completed = Collector<Bool>()
        let sub = pipe.asyncRay
            .timeout(.seconds(5), clock: clock)
            .sink(next: { values.append($0) }, completed: { completed.append(true) })
        defer { sub.cancel() }
        #expect(await eventually { clock.sleepCount == 1 && pipe.subscriberCount == 1 })

        clock.advance(by: .seconds(4))
        pipe.send(1)
        #expect(await eventually { values.values == [1] && clock.sleepCount == 2 })

        // Re-armed at 4s, so the deadline is now 9s.
        clock.advance(by: .seconds(4))
        #expect(clock.pendingSleepers == 1)
        #expect(completed.values.isEmpty)

        clock.advance(by: .seconds(1))
        #expect(await eventually { completed.values == [true] })
    }

    @Test func timerCompletesAtDeadline() async {
        let clock = ManualClock()
        let completed = Collector<Bool>()
        let sub = AsyncRay<Int>.timer(.seconds(10), clock: clock)
            .sink(next: { _ in }, completed: { completed.append(true) })
        defer { sub.cancel() }
        #expect(await eventually { clock.sleepCount == 1 })

        clock.advance(by: .seconds(9))
        #expect(completed.values.isEmpty)

        clock.advance(by: .seconds(1))
        #expect(await eventually { completed.values == [true] })
    }

    @Test func cancellingSubscriptionReleasesSleeper() async {
        let clock = ManualClock()
        let sub = AsyncRay<Int>.timer(.seconds(10), clock: clock).sink { _ in }
        #expect(await eventually { clock.pendingSleepers == 1 })

        sub.cancel()
        #expect(await eventually { clock.pendingSleepers == 0 })
    }
}
