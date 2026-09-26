# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added
- Added `swift-format` configuration plus Makefile targets to lint and format selected Swift paths.
- Expanded public API documentation for cancellation, buffering, ordering, completion, and stream bridging behavior.

### Changed
- Applied the configured Swift formatting to source and test files; removed redundant file-title comments above imports.
- Updated `test-phone` to use the available iPhone 18 Pro simulator destination.

## [1.0.0] - 2026-09-25

First release of AsyncRay.

### Added
- Cold `AsyncRay<T>` streams built on `AsyncStream`, with `AsyncRayEmitter` for custom producers.
- Hot event and state primitives: `Pipe`, `CurrentValue`, `CurrentValueDistinct`, and `Once`.
- Stream operators for transformation, filtering, timing, combining, dispatching, and side effects, including `merge`, `combineLatest`, `zip`, `flatMap`, `flatMapLatest`, `scan`, and `reduce`.
- AsyncSequence support and bridges to and from `AsyncStream`, including a lazy shared bridge with automatic cancellation when the last subscriber disconnects.
- Subscription lifecycle tools: `Subscription`, auto-pruning `SubscriptionBag`, cancellation-safe cleanup handlers, and atomic task management.
- Buffering policy propagation for unary operators and explicit buffering policies for fan-in operators.
- Swift Package Manager and Bazel support, with complete Swift 6 concurrency checking and a deterministic test suite.
- `flatMapLatest` now keeps its latest inner stream alive after the outer stream completes; the output finishes after the inner stream completes.
- `delay` now shifts each value's delivery time independently instead of serializing delays.
- Swift 6 toolchains select Swift 6 language mode while Swift 5 language-mode support remains available.

### Compatibility
- Existing Flux clients need to update the package, module, primary type, and related API names to use AsyncRay.
- Deployment targets remain macOS 14, iOS 16, tvOS 16, watchOS 9, and visionOS 1.
- The package supports Swift 5 language mode and selects Swift 6 language mode with Swift 6 toolchains.
