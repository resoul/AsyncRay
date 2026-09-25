import Foundation

/// Controls which platform-provided safe-area edges are applied to Prism layout.
public enum SafeAreaPolicy: Sendable, Equatable {
    case all
    case topOnly
    case bottomOnly
    case none
}
