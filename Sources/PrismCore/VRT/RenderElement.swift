import Foundation

/// Pure, immutable value representing a node in the Virtual Render Tree.
///
/// Invariant: Must never contain references to CALayer, UIView, NSView, or host platform objects.
public struct RenderElement: Equatable, Sendable, CustomStringConvertible {
    public var id: ElementID
    public var kind: ElementKind
    public var props: ElementProps
    public var modifiers: [ElementModifier]
    public var children: [RenderElement]

    public init(
        id: ElementID,
        kind: ElementKind,
        props: ElementProps = ElementProps(),
        modifiers: [ElementModifier] = [],
        children: [RenderElement] = []
    ) {
        self.id = id
        self.kind = kind
        self.props = props
        self.modifiers = modifiers
        self.children = children
    }

    public var resolvedStyle: ResolvedStyle {
        ResolvedStyle.resolve(from: modifiers)
    }

    public var description: String {
        dumpTree()
    }

    /// Normalizes the element tree:
    /// - Inlines structural `Group` children
    /// - Prunes `Empty` children
    /// - Reassigns sibling indices deterministically
    public func normalized() -> RenderElement {
        var normalizedChildren: [RenderElement] = []

        func collect(_ elements: [RenderElement]) {
            for element in elements {
                let normalizedElement = element.normalized()
                switch normalizedElement.kind {
                case .empty:
                    continue
                case .group:
                    collect(normalizedElement.children)
                default:
                    normalizedChildren.append(normalizedElement)
                }
            }
        }

        collect(children)

        let reindexedChildren = normalizedChildren.enumerated().map { index, child in
            var updated = child
            updated.id = child.id.withSiblingIndex(index)
            return updated
        }

        var copy = self
        copy.children = reindexedChildren
        return copy
    }

    /// Generates a human-readable and testable visual dump of the element tree.
    public func dumpTree(indent: Int = 0) -> String {
        let prefix = String(repeating: "  ", count: indent)
        var line = "\(prefix)\(kind)"

        if let key = id.key {
            line += " [key: \"\(key)\"]"
        }

        if id.siblingIndex > 0 {
            line += " [sibling: \(id.siblingIndex)]"
        }

        if let testID = props.testID {
            line += " [testID: \"\(testID)\"]"
        }

        if !modifiers.isEmpty {
            let modifierDescriptions = modifiers.map { $0.description }.joined(separator: ", ")
            line += " {\(modifierDescriptions)}"
        }

        if children.isEmpty {
            return line
        }

        let childLines = children.map { $0.dumpTree(indent: indent + 1) }.joined(separator: "\n")
        return "\(line)\n\(childLines)"
    }
}

// MARK: - Modifiers on RenderElement

extension RenderElement {
    public func width(_ value: Double) -> RenderElement {
        var copy = self
        copy.modifiers.append(.width(value))
        return copy
    }

    public func height(_ value: Double) -> RenderElement {
        var copy = self
        copy.modifiers.append(.height(value))
        return copy
    }

    public func frame(width: Double? = nil, height: Double? = nil) -> RenderElement {
        var copy = self
        if let width { copy.modifiers.append(.width(width)) }
        if let height { copy.modifiers.append(.height(height)) }
        return copy
    }

    public func minWidth(_ value: Double) -> RenderElement {
        var copy = self
        copy.modifiers.append(.minWidth(value))
        return copy
    }

    public func maxWidth(_ value: Double) -> RenderElement {
        var copy = self
        copy.modifiers.append(.maxWidth(value))
        return copy
    }

    public func minHeight(_ value: Double) -> RenderElement {
        var copy = self
        copy.modifiers.append(.minHeight(value))
        return copy
    }

    public func maxHeight(_ value: Double) -> RenderElement {
        var copy = self
        copy.modifiers.append(.maxHeight(value))
        return copy
    }

    public func padding(_ insets: DirectionalEdgeInsets) -> RenderElement {
        var copy = self
        copy.modifiers.append(.padding(insets))
        return copy
    }

    public func padding(_ all: Double) -> RenderElement {
        padding(DirectionalEdgeInsets(all: all))
    }

    public func padding(
        top: Double = 0,
        leading: Double = 0,
        bottom: Double = 0,
        trailing: Double = 0
    ) -> RenderElement {
        padding(DirectionalEdgeInsets(top: top, leading: leading, bottom: bottom, trailing: trailing))
    }

    public func margin(_ insets: DirectionalEdgeInsets) -> RenderElement {
        var copy = self
        copy.modifiers.append(.margin(insets))
        return copy
    }

    public func margin(_ all: Double) -> RenderElement {
        margin(DirectionalEdgeInsets(all: all))
    }

    public func margin(
        top: Double = 0,
        leading: Double = 0,
        bottom: Double = 0,
        trailing: Double = 0
    ) -> RenderElement {
        margin(DirectionalEdgeInsets(top: top, leading: leading, bottom: bottom, trailing: trailing))
    }

    public func background(_ color: Color) -> RenderElement {
        var copy = self
        copy.modifiers.append(.background(color))
        return copy
    }

    public func foregroundColor(_ color: Color) -> RenderElement {
        var copy = self
        copy.modifiers.append(.foreground(color))
        return copy
    }

    public func opacity(_ value: Double) -> RenderElement {
        var copy = self
        copy.modifiers.append(.opacity(value))
        return copy
    }

    public func zIndex(_ value: Int) -> RenderElement {
        var copy = self
        copy.modifiers.append(.zIndex(value))
        return copy
    }

    public func key(_ key: String) -> RenderElement {
        var copy = self
        copy.id = copy.id.withKey(key)
        copy.modifiers.append(.explicitKey(key))
        return copy
    }

    public func testID(_ id: String) -> RenderElement {
        var copy = self
        copy.props.testID = id
        copy.modifiers.append(.testID(id))
        if let act = ActionRegistry.shared.action(for: self.id) {
            ActionRegistry.shared.register(action: act, forTestID: id)
        }
        return copy
    }

    public func onTap(_ action: @escaping @MainActor () -> Void) -> RenderElement {
        ActionRegistry.shared.register(action: action, for: self.id)
        if let testID = self.props.testID {
            ActionRegistry.shared.register(action: action, forTestID: testID)
        }
        var copy = self
        copy.props.custom["isButton"] = "true"
        return copy
    }

    public func accessibilityLabel(_ label: String) -> RenderElement {
        var copy = self
        copy.props.accessibilityLabel = label
        return copy
    }

    public func accessibilityHint(_ hint: String) -> RenderElement {
        var copy = self
        copy.props.custom["accessibilityHint"] = hint
        return copy
    }

    public func accessibilityValue(_ value: String) -> RenderElement {
        var copy = self
        copy.props.custom["accessibilityValue"] = value
        return copy
    }

    public func accessibilityTraits(_ traits: AccessibilityTraits) -> RenderElement {
        var copy = self
        copy.props.custom["accessibilityTraits"] = String(traits.rawValue)
        return copy
    }

    public func accessibilityElement(label: String, role: String? = nil) -> RenderElement {
        var copy = self
        copy.props.accessibilityLabel = label
        if let role {
            copy.props.custom["accessibilityRole"] = role
            copy.props.custom["role"] = role
        }
        return copy
    }

    public func render(in context: ComponentContext = .default) -> RenderElement {
        self
    }

    public func focusable(_ isFocusable: Bool = true, order: Int? = nil) -> RenderElement {
        var copy = self
        copy.props.custom["isFocusable"] = isFocusable ? "true" : "false"
        if let order {
            copy.props.custom["focusOrder"] = String(order)
        } else {
            copy.props.custom.removeValue(forKey: "focusOrder")
        }
        return copy
    }

    public func clipped(_ isClipped: Bool = true) -> RenderElement {
        var copy = self
        copy.props.custom["clip"] = isClipped ? "true" : "false"
        return copy
    }

    public var transition: Transition? {
        for modifier in modifiers.reversed() {
            if case .transition(let t) = modifier {
                return t
            }
        }
        return nil
    }

    public var animation: Animation? {
        for modifier in modifiers.reversed() {
            if case .animation(let a) = modifier {
                return a
            }
        }
        return nil
    }

    public func transition(_ transition: Transition) -> RenderElement {
        var copy = self
        copy.modifiers.append(.transition(transition))
        return copy
    }

    public func animation(_ animation: Animation?) -> RenderElement {
        var copy = self
        copy.modifiers.append(.animation(animation))
        return copy
    }

    /// Applies an anti-aliased Signed Distance Field rounded rectangle effect.
    /// On devices without Metal support, falls back gracefully to standard CALayer cornerRadius and borderWidth.
    public func sdfRoundedRect(
        cornerRadius: Double,
        borderWidth: Double = 0,
        borderColor: Color = .clear,
        fill: Color? = nil
    ) -> RenderElement {
        var copy = self
        copy.modifiers.append(.sdfRoundedRect(cornerRadius: cornerRadius, borderWidth: borderWidth, borderColor: borderColor, fill: fill))
        return copy
    }

    /// Applies a frosted glassmorphism effect with blur, saturation boost, and tint overlay.
    /// On devices without Metal support, falls back to a tinted semi-transparent overlay.
    public func glassmorphism(
        blurRadius: Double = 20,
        tint: Color = Color(red: 1, green: 1, blue: 1, alpha: 0.2),
        saturation: Double = 1.2
    ) -> RenderElement {
        var copy = self
        copy.modifiers.append(.glassmorphism(blurRadius: blurRadius, tint: tint, saturation: saturation))
        return copy
    }

    /// Applies a multi-color mesh gradient effect.
    /// On devices without Metal support, falls back to a multi-stop linear gradient.
    public func meshGradient(_ grid: MeshGradientGrid) -> RenderElement {
        var copy = self
        copy.modifiers.append(.meshGradient(grid))
        return copy
    }
}
