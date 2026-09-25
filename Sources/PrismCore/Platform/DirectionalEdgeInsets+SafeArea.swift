import Foundation

public extension DirectionalEdgeInsets {
    func applying(_ policy: SafeAreaPolicy) -> DirectionalEdgeInsets {
        switch policy {
        case .all: return self
        case .topOnly: return .init(top: top, leading: 0, bottom: 0, trailing: 0)
        case .bottomOnly: return .init(top: 0, leading: 0, bottom: bottom, trailing: 0)
        case .none: return .zero
        }
    }
}
