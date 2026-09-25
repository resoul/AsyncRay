# Prism Manual Testing

This workspace hosts two intentionally small consumer apps for hands-on component testing:

- `iOS/iOS.xcodeproj` — iPhone and iPad host using `HostUIView`.
- `macOS/macOS.xcodeproj` — resizable AppKit host using `HostNSView`.

Both projects resolve the local Prism package from `../../..` and render the same focused Layout + Text screen. Resize the macOS window or rotate the iOS simulator to inspect stack spacing, alignment, flexible spacers, fixed frames, dividers, backgrounds, typography, and text wrapping.

Open `PrismManualTesting.xcworkspace` and select either app scheme. No CI or snapshot assertions are included yet; this example is deliberately optimized for visual/manual iteration.
