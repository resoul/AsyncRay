import Testing
@testable import AsyncRay

/// Sends `value` until the subscriber's one-element buffer accepts it. With
/// `.bufferingOldest(1)`, acceptance proves the subscriber already read the previous value.
private func sendWhenRead<T: Sendable>(_ pipe: Pipe<T>, _ value: T) async -> Bool {
    await eventually {
        if case .enqueued = pipe.sendObservingOverflow(value).first { return true }
        return false
    }
}

@Suite(.timeLimit(.minutes(1)))
struct MoreOperatorsTests {

    // MARK: - throttle(latest:)

    @Test func throttleLatestFalseIsLeadingThrottle() async {
        let clock = ManualClock()
        let values = await AsyncRay.from([1, 2, 3])
            .throttle(.seconds(1), latest: false, clock: clock)
            .collect()
        #expect(values == [1])
    }

    @Test func throttleLatestForwardsLatestValueOfWindow() async {
        let clock = ManualClock()
        let pipe = Pipe<Int>(bufferingPolicy: .bufferingOldest(1))
        let values = Collector<Int>()
        let sub = pipe.asyncRay.throttle(.seconds(1), latest: true, clock: clock)
            .sink { values.append($0) }
        defer { sub.cancel() }
        #expect(await eventually { pipe.subscriberCount == 1 })

        #expect(await sendWhenRead(pipe, 1))
        #expect(await eventually { values.values == [1] && clock.sleepCount == 1 })
        #expect(await sendWhenRead(pipe, 2))
        #expect(await sendWhenRead(pipe, 3))  // 2 has been read
        #expect(await sendWhenRead(pipe, 3))  // the first 3 has been read

        clock.advance(by: .seconds(1))
        #expect(await eventually { values.values.last == 3 })
        // The repeated 3 may land in the next window; 2 never surfaces.
        clock.advance(by: .seconds(1))
        #expect(values.values.first == 1)
        #expect(!values.values.contains(2))
    }

    @Test func throttleLatestDeliversPendingValueBeforeCompleting() async {
        let clock = ManualClock()
        let done = Collector<[Int]>()
        let collecting = Task {
            done.append(
                await AsyncRay.from([1, 2]).throttle(.seconds(1), latest: true, clock: clock)
                    .collect()
            )
        }
        #expect(await eventually { clock.sleepCount == 1 })
        for _ in 0..<10 where done.count() == 0 {
            clock.advance(by: .seconds(1))
            _ = await eventually { done.count() == 1 || clock.pendingSleepers == 1 }
        }
        await collecting.value
        #expect(done.values == [[1, 2]])
    }

    // MARK: - interval

    @Test func intervalTicksOnFixedDeadlines() async {
        let clock = ManualClock()
        let ticks = Collector<Int>()
        let sub = AsyncRay.interval(.seconds(1), clock: clock).sink { ticks.append($0) }
        defer { sub.cancel() }

        #expect(await eventually { clock.pendingSleepers == 1 })
        clock.advance(by: .milliseconds(999))
        #expect(ticks.values.isEmpty)
        clock.advance(by: .milliseconds(1))
        #expect(await eventually { ticks.values == [0] })

        #expect(await eventually { clock.pendingSleepers == 1 })
        clock.advance(by: .seconds(1))
        #expect(await eventually { ticks.values == [0, 1] })

        // Missed deadlines fire immediately; the newest tick always arrives.
        #expect(await eventually { clock.pendingSleepers == 1 })
        clock.advance(by: .seconds(3))
        #expect(await eventually { ticks.values.last == 4 })
    }

    @Test func intervalStopsOnCancellation() async {
        let clock = ManualClock()
        let sub = AsyncRay.interval(.seconds(1), clock: clock).sink { _ in }
        #expect(await eventually { clock.pendingSleepers == 1 })
        sub.cancel()
        #expect(await eventually { clock.pendingSleepers == 0 })
    }

    // MARK: - chunks

    @Test func chunksGroupValuesAndFlushRemainder() async {
        let chunks = await AsyncRay.from(1...5).chunks(ofCount: 2).collect()
        #expect(chunks == [[1, 2], [3, 4], [5]])
    }

    @Test func chunksDoNotEmitEmptyTrailingChunk() async {
        let chunks = await AsyncRay.from(1...4).chunks(ofCount: 2).collect()
        #expect(chunks == [[1, 2], [3, 4]])
        #expect(await AsyncRay<Int>.empty().chunks(ofCount: 3).collect().isEmpty)
    }

    // MARK: - skipRepeats(by:)

    @Test func skipRepeatsByUsesCustomEquivalence() async {
        struct User: Sendable {
            let id: Int
            let name: String
        }
        let users = [
            User(id: 1, name: "a"), User(id: 1, name: "b"), User(id: 2, name: "c"),
            User(id: 1, name: "d"),
        ]
        let names = await AsyncRay.from(users)
            .skipRepeats { $0.id == $1.id }
            .map(\.name)
            .collect()
        #expect(names == ["a", "c", "d"])
    }

    // MARK: - prepend

    @Test func prependEmitsValuesBeforeUpstream() async {
        #expect(await AsyncRay.from([3, 4]).prepend(1, 2).collect() == [1, 2, 3, 4])
        #expect(await AsyncRay<Int>.empty().prepend([7]).collect() == [7])
    }

    // MARK: - withLatestFrom

    @Test func withLatestFromDropsValuesBeforeOtherEmits() async {
        let pairs = await AsyncRay.from([1, 2])
            .withLatestFrom(AsyncRay<String>.never())
            .collect()
        #expect(pairs.isEmpty)
    }

    @Test func withLatestFromPairsWithLatestOtherValue() async {
        let pipe = Pipe<Int>()
        let pairs = Collector<String>()
        let sub = pipe.asyncRay.withLatestFrom(AsyncRay.just("x"))
            .sink { pairs.append("\($0)\($1)") }
        defer { sub.cancel() }
        #expect(await eventually { pipe.subscriberCount == 1 })

        // The other stream is read concurrently; probe until its value is visible.
        #expect(
            await eventually {
                pipe.send(0)
                return pairs.values.contains("0x")
            }
        )
        pipe.send(5)
        #expect(await eventually { pairs.values.last == "5x" })
    }

    @Test func withLatestFromCompletesWithSourceAndCancelsOther() async {
        let other = Pipe<String>()
        let pairs = await AsyncRay<Int>.empty().withLatestFrom(other.asyncRay).collect()
        #expect(pairs.isEmpty)
        #expect(await eventually { other.subscriberCount == 0 })
    }
}
