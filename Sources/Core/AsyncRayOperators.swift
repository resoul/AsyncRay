// Stream insertion operator <- for Pipe<T>

infix operator <-: AssignmentPrecedence

/// Sends a value into a `Pipe`.
///
/// ```swift
/// let events = Pipe<String>()
/// events <- "Hello"
/// events <- "World"
///
/// // Equivalent to:
/// events.send("Hello")
/// events.send("World")
/// ```
@discardableResult
public func <- <T>(pipe: Pipe<T>, value: T) -> Pipe<T> {
    pipe.send(value)
    return pipe
}

/// Sends an array of values into a `Pipe`.
///
/// ```swift
/// events <- ["a", "b", "c"]
/// ```
@discardableResult
public func <- <T>(pipe: Pipe<T>, values: [T]) -> Pipe<T> {
    values.forEach { pipe.send($0) }
    return pipe
}
