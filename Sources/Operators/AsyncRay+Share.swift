extension AsyncRay {

    // MARK: - share

    /// Shares one upstream subscription among all subscribers of the returned stream.
    ///
    /// The upstream subscription starts when the first subscriber connects and is cancelled when
    /// the last one disconnects. If upstream completes, every current subscriber completes. In
    /// both cases the next subscriber starts a fresh upstream subscription, so a shared network
    /// request runs once per connection instead of once per subscriber.
    ///
    /// Values are not replayed: a subscriber receives only values produced after it connects.
    /// Registration is asynchronous, so values produced right after `sink` returns may be missed;
    /// use `CurrentValue` when late subscribers need the latest value. Each subscriber has its
    /// own buffer using `bufferingPolicy`, so a slow subscriber may drop values without slowing
    /// the others.
    ///
    /// ```swift
    /// let profile = api.fetchProfile().share()
    /// profile.sinkOnMain { header.update($0) }.store(in: bag)
    /// profile.sinkOnMain { avatar.update($0) }.store(in: bag)  // reuses the same request
    /// ```
    public func share(
        bufferingPolicy: AsyncStream<T>.Continuation.BufferingPolicy = .bufferingNewest(64)
    ) -> AsyncRay<T> {
        SharedAsyncRay(restartable: _make, bufferingPolicy: bufferingPolicy).asyncRay
    }
}
