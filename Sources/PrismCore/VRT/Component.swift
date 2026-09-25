import Foundation

/// Execution context passed to components during tree construction.
public struct ComponentContext: Sendable {
    public var environment: LocalizationEnvironment
    public var theme: Theme?
    public var values: [String: Sendable]
    public var screenState: [String: Sendable]

    public init(
        environment: LocalizationEnvironment = LocalizationEnvironment(),
        theme: Theme? = nil,
        values: [String: Sendable] = [:]
    ) {
        self.environment = environment
        self.theme = theme
        self.values = values
        self.screenState = [:]
    }

    public init(
        environment: LocalizationEnvironment = LocalizationEnvironment(),
        theme: Theme? = nil,
        values: [String: Sendable] = [:],
        screenState: [String: Sendable]
    ) {
        self.environment = environment
        self.theme = theme
        self.values = values
        self.screenState = screenState
    }

    public static let `default` = ComponentContext()
}


/// Abstract protocol for any type that can be converted into an array of RenderElements.
public protocol ComponentConvertible: Sendable {
    func asRenderElements(in context: ComponentContext) -> [RenderElement]
}

extension ComponentConvertible {
    public func asRenderElements() -> [RenderElement] {
        asRenderElements(in: .default)
    }
}

/// Declarative user component protocol.
public protocol Component: ComponentConvertible {
    func body(context: ComponentContext) -> RenderElement
}

extension Component {
    public func asRenderElements(in context: ComponentContext) -> [RenderElement] {
        [body(context: context)]
    }

    public func render(in context: ComponentContext = .default) -> RenderElement {
        body(context: context)
    }
}

extension RenderElement: ComponentConvertible {
    public func asRenderElements(in context: ComponentContext) -> [RenderElement] {
        [self]
    }
}

extension Array: ComponentConvertible where Element == RenderElement {
    public func asRenderElements(in context: ComponentContext) -> [RenderElement] {
        self
    }
}

// MARK: - Modifiers on Component

extension Component {
    public func width(_ value: Double) -> RenderElement {
        render().width(value)
    }

    public func height(_ value: Double) -> RenderElement {
        render().height(value)
    }

    public func frame(width: Double? = nil, height: Double? = nil) -> RenderElement {
        render().frame(width: width, height: height)
    }

    public func minWidth(_ value: Double) -> RenderElement {
        render().minWidth(value)
    }

    public func maxWidth(_ value: Double) -> RenderElement {
        render().maxWidth(value)
    }

    public func minHeight(_ value: Double) -> RenderElement {
        render().minHeight(value)
    }

    public func maxHeight(_ value: Double) -> RenderElement {
        render().maxHeight(value)
    }

    public func padding(_ insets: DirectionalEdgeInsets) -> RenderElement {
        render().padding(insets)
    }

    public func padding(_ all: Double) -> RenderElement {
        render().padding(all)
    }

    public func padding(
        top: Double = 0,
        leading: Double = 0,
        bottom: Double = 0,
        trailing: Double = 0
    ) -> RenderElement {
        render().padding(top: top, leading: leading, bottom: bottom, trailing: trailing)
    }

    public func margin(_ insets: DirectionalEdgeInsets) -> RenderElement {
        render().margin(insets)
    }

    public func margin(_ all: Double) -> RenderElement {
        render().margin(all)
    }

    public func margin(
        top: Double = 0,
        leading: Double = 0,
        bottom: Double = 0,
        trailing: Double = 0
    ) -> RenderElement {
        render().margin(top: top, leading: leading, bottom: bottom, trailing: trailing)
    }

    public func background(_ color: Color) -> RenderElement {
        render().background(color)
    }

    /// Applies text/content color without affecting the element's surface background.
    public func foregroundColor(_ color: Color) -> RenderElement {
        render().foregroundColor(color)
    }

    public func opacity(_ value: Double) -> RenderElement {
        render().opacity(value)
    }

    public func zIndex(_ value: Int) -> RenderElement {
        render().zIndex(value)
    }

    public func key(_ key: String) -> RenderElement {
        render().key(key)
    }

    public func testID(_ id: String) -> RenderElement {
        render().testID(id)
    }

    public func accessibilityLabel(_ label: String) -> RenderElement {
        render().accessibilityLabel(label)
    }

    public func accessibilityHint(_ hint: String) -> RenderElement {
        render().accessibilityHint(hint)
    }

    public func accessibilityValue(_ value: String) -> RenderElement {
        render().accessibilityValue(value)
    }

    public func accessibilityTraits(_ traits: AccessibilityTraits) -> RenderElement {
        render().accessibilityTraits(traits)
    }

    public func accessibilityElement(label: String, role: String? = nil) -> RenderElement {
        render().accessibilityElement(label: label, role: role)
    }

    public func focusable(_ isFocusable: Bool = true, order: Int? = nil) -> RenderElement {
        render().focusable(isFocusable, order: order)
    }

    public func clipped(_ isClipped: Bool = true) -> RenderElement {
        render().clipped(isClipped)
    }

    public func transition(_ transition: Transition) -> RenderElement {
        render().transition(transition)
    }

    public func animation(_ animation: Animation?) -> RenderElement {
        render().animation(animation)
    }
}
