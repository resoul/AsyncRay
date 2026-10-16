import Testing
@testable import AsyncRay

@Suite(.timeLimit(.minutes(1)))
struct ShareTests {

    @Test func subscribersShareOneUpstreamSubscription() async {
        let pipe = Pipe<Int>()
        let shared = pipe.asyncRay.map { $0 * 10 }.share()
        let r1 = Collector<Int>()
        let r2 = Collector<Int>()

        let s1 = shared.sink { r1.append($0) }
        defer { s1.cancel() }
        #expect(await eventually { pipe.subscriberCount == 1 })
        let s2 = shared.sink { r2.append($0) }
        defer { s2.cancel() }
        // Registration is asynchronous; send a marker until the second subscriber sees it.
        #expect(
            await eventually {
                pipe.send(0)
                return r2.values.contains(0)
            }
        )

        pipe.send(1)
        #expect(await eventually { r1.values.last == 10 && r2.values.last == 10 })
        #expect(pipe.subscriberCount == 1)
    }

    @Test func lastSubscriberLeavingCancelsUpstreamAndNextOneReconnects() async {
        let pipe = Pipe<Int>()
        let shared = pipe.asyncRay.share()

        let s1 = shared.sink { _ in }
        let s2 = shared.sink { _ in }
        #expect(await eventually { pipe.subscriberCount == 1 })

        s1.cancel()
        s2.cancel()
        #expect(await eventually { pipe.subscriberCount == 0 })

        let received = Collector<Int>()
        let s3 = shared.sink { received.append($0) }
        defer { s3.cancel() }
        #expect(await eventually { pipe.subscriberCount == 1 })
        pipe.send(7)
        #expect(await eventually { received.values == [7] })
    }

    @Test func completedUpstreamRestartsForNextSubscriber() async {
        let starts = Collector<Bool>()
        let shared = AsyncRay<Int> { emitter in
            starts.append(true)
            for value in [1, 2, 3] { emitter.send(value) }
            emitter.finish()
        }.share()

        #expect(await shared.collect() == [1, 2, 3])
        #expect(await shared.collect() == [1, 2, 3])
        #expect(starts.count() == 2)
    }

    @Test func immediateCancellationDoesNotLeaveUpstreamRunning() async {
        let pipe = Pipe<Int>()
        let shared = pipe.asyncRay.share()

        for _ in 0..<200 {
            let sub = shared.sink { _ in }
            sub.cancel()
        }
        #expect(await eventually { pipe.subscriberCount == 0 })
    }
}
