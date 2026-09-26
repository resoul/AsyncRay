import Testing
import Foundation
@testable import AsyncRay

// MARK: - onMain

@Test func onMainEmitsValues() async {
    let asyncRay = AsyncRay.from([1, 2, 3]).onMain()
    let values = await asyncRay.collect()
    #expect(values == [1, 2, 3])
}

@Test func onMainCancellation() async {
    let pipe = Pipe<Int>()
    let received = Collector<Int>()

    let sub = pipe.asyncRay.onMain().sink { v in
        received.append(v)
    }

    try? await Task.sleep(for: .milliseconds(10))
    pipe.send(1)
    try? await Task.sleep(for: .milliseconds(20))
    sub.cancel()
    pipe.send(2)
    try? await Task.sleep(for: .milliseconds(20))

    let values = received.values
    #expect(values == [1])
}

@Test func sinkOnMainRunsOnMainActor() async {
    let isMainCollector = Collector<Bool>()
    let sub = await MainActor.run {
        AsyncRay.from([1, 2, 3]).sinkOnMain { _ in
            let isMain = Thread.isMainThread
            isMainCollector.append(isMain)
        }
    }

    try? await Task.sleep(for: .milliseconds(50))
    sub.cancel()

    let values = isMainCollector.values
    #expect(values == [true, true, true])
}

// MARK: - onBackground

@Test func onBackgroundEmitsValues() async {
    let asyncRay = AsyncRay.from([10, 20, 30]).onBackground(priority: .utility)
    let values = await asyncRay.collect()
    #expect(values == [10, 20, 30])
}

@Test func onBackgroundCancellationStopsStream() async {
    let pipe = Pipe<Int>()
    let received = Collector<Int>()

    let sub = pipe.asyncRay
        .onBackground()
        .sink { v in
            received.append(v)
        }

    try? await Task.sleep(for: .milliseconds(20))
    pipe.send(1)
    try? await Task.sleep(for: .milliseconds(20))
    sub.cancel()
    pipe.send(2)
    try? await Task.sleep(for: .milliseconds(20))

    let values = received.values
    #expect(values == [1])
}

// MARK: - handleEvents

@Test func handleEventsExecutesSideEffect() async {
    let sideEffects = Collector<Int>()
    let results = await AsyncRay.from([1, 2, 3])
        .handleEvents { val in
            sideEffects.append(val * 10)
        }
        .collect()

    try? await Task.sleep(for: .milliseconds(30))
    let sideValues = sideEffects.values

    #expect(results == [1, 2, 3])
    #expect(sideValues == [10, 20, 30])
}

// MARK: - onCompletion

@Test func onCompletionExecutesWhenStreamFinishes() async {
    let completedFired = Collector<Bool>()
    let results = await AsyncRay.from(["a", "b"])
        .onCompletion {
            completedFired.append(true)
        }
        .collect()

    try? await Task.sleep(for: .milliseconds(30))
    let fired = completedFired.values

    #expect(results == ["a", "b"])
    #expect(fired == [true])
}

@Test func onCompletionDoesNotExecuteWhenCancelled() async {
    let completedFired = Collector<Bool>()
    let pipe = Pipe<Int>()

    let sub = pipe.asyncRay
        .onCompletion {
            completedFired.append(true)
        }
        .sink { _ in }

    pipe.send(1)
    try? await Task.sleep(for: .milliseconds(10))
    sub.cancel()
    pipe.finish()
    try? await Task.sleep(for: .milliseconds(30))

    let fired = completedFired.values
    #expect(fired.isEmpty)
}

@Test func onCompletionCancelBeforeUpstreamFinish() async {
    let completedFired = Collector<Bool>()
    let asyncRay = AsyncRay<Int> { emitter in
        Task {
            emitter.send(1)
            try? await Task.sleep(for: .milliseconds(50))
            emitter.finish()
        }
    }

    let sub =
        asyncRay
        .onCompletion {
            completedFired.append(true)
        }
        .sink { _ in }

    try? await Task.sleep(for: .milliseconds(10))
    sub.cancel()
    try? await Task.sleep(for: .milliseconds(60))

    let fired = completedFired.values
    #expect(fired.isEmpty)
}
