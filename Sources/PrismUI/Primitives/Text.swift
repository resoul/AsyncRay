import Foundation
import PrismCore

/// Declarative text primitive for displaying static or localized text.
public struct Text: Component {
    public enum Content: Sendable, Equatable {
        case verbatim(String)
        case localized(LocalizedStringKey)
    }

    public let content: Content
    public var fontRole: FontRole?
    public var textColor: Color?
    public var lineLimit: Int?
    public var alignment: HorizontalAlignment

    public init(_ string: String) {
        self.content = .verbatim(string)
        self.fontRole = nil
        self.textColor = nil
        self.lineLimit = nil
        self.alignment = .leading
    }

    public init(_ key: LocalizedStringKey) {
        self.content = .localized(key)
        self.fontRole = nil
        self.textColor = nil
        self.lineLimit = nil
        self.alignment = .leading
    }

    public func body(context: ComponentContext) -> RenderElement {
        let textString: String
        switch content {
        case .verbatim(let s):
            textString = s
        case .localized(let key):
            textString = context.environment.localized(key).string
        }

        var props = ElementProps()
        if let lineLimit {
            props.custom["lineLimit"] = String(lineLimit)
        }
        if let fontRole {
            props.custom["fontRole"] = fontRole.description
        }
        props.custom["alignment"] = alignment.rawValue

        var modifiers: [ElementModifier] = []
        if let textColor {
            modifiers.append(.foreground(textColor))
        }

        return RenderElement(
            id: ElementID(typeName: "Text"),
            kind: .text(textString),
            props: props,
            modifiers: modifiers
        )
    }

    public func font(_ role: FontRole) -> Text {
        var copy = self
        copy.fontRole = role
        return copy
    }

    public func foregroundColor(_ color: Color) -> Text {
        var copy = self
        copy.textColor = color
        return copy
    }

    public func lineLimit(_ limit: Int?) -> Text {
        var copy = self
        copy.lineLimit = limit
        return copy
    }

    public func alignment(_ alignment: HorizontalAlignment) -> Text {
        var copy = self
        copy.alignment = alignment
        return copy
    }
}
