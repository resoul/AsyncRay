import Foundation
@_exported import PrismCore

/// Policy for safe-area insetting applied to Scaffold content.
/// Automatic scrolling behavior for primary Scaffold content.
public enum AutoScrollPolicy: Sendable, Equatable {
    case automatic
    case disabled
}

/// Primary app structure scaffold coordinating top bar, bottom navigation, sidebar, and content area.
public struct Scaffold: Component {
    public var topBar: (any ComponentConvertible)?
    public var bottomBar: (any ComponentConvertible)?
    public var sidebar: (any ComponentConvertible)?
    public var overlay: (any ComponentConvertible)?
    public var content: any ComponentConvertible
    public var safeAreaPolicy: SafeAreaPolicy
    public var autoScrollPolicy: AutoScrollPolicy

    public init(
        safeAreaPolicy: SafeAreaPolicy = .all,
        autoScrollPolicy: AutoScrollPolicy = .disabled,
        topBar: (any ComponentConvertible)? = nil,
        bottomBar: (any ComponentConvertible)? = nil,
        sidebar: (any ComponentConvertible)? = nil,
        overlay: (any ComponentConvertible)? = nil,
        content: any ComponentConvertible
    ) {
        self.safeAreaPolicy = safeAreaPolicy
        self.autoScrollPolicy = autoScrollPolicy
        self.topBar = topBar
        self.bottomBar = bottomBar
        self.sidebar = sidebar
        self.overlay = overlay
        self.content = content
    }

    public init(
        safeAreaPolicy: SafeAreaPolicy = .all,
        autoScrollPolicy: AutoScrollPolicy = .disabled,
        topBar: (any ComponentConvertible)? = nil,
        bottomBar: (any ComponentConvertible)? = nil,
        sidebar: (any ComponentConvertible)? = nil,
        overlay: (any ComponentConvertible)? = nil,
        @ComponentBuilder content: () -> [RenderElement]
    ) {
        self.init(
            safeAreaPolicy: safeAreaPolicy,
            autoScrollPolicy: autoScrollPolicy,
            topBar: topBar,
            bottomBar: bottomBar,
            sidebar: sidebar,
            overlay: overlay,
            content: content()
        )
    }

    // MARK: - Fluent Slot Modifiers

    public func topBar(@ComponentBuilder _ builder: () -> [RenderElement]) -> Scaffold {
        var copy = self
        copy.topBar = builder()
        return copy
    }

    public func bottomBar(@ComponentBuilder _ builder: () -> [RenderElement]) -> Scaffold {
        var copy = self
        copy.bottomBar = builder()
        return copy
    }

    public func sidebar(@ComponentBuilder _ builder: () -> [RenderElement]) -> Scaffold {
        var copy = self
        copy.sidebar = builder()
        return copy
    }

    public func overlay(@ComponentBuilder _ builder: () -> [RenderElement]) -> Scaffold {
        var copy = self
        copy.overlay = builder()
        return copy
    }

    public func autoScroll(_ policy: AutoScrollPolicy) -> Scaffold {
        var copy = self
        copy.autoScrollPolicy = policy
        return copy
    }

    public func safeArea(_ policy: SafeAreaPolicy) -> Scaffold {
        var copy = self
        copy.safeAreaPolicy = policy
        return copy
    }

    // MARK: - Component Body

    public func body(context: ComponentContext) -> RenderElement {
        let contentElements = content.asRenderElements(in: context)

        // Middle horizontal section: [Sidebar (optional) + Content]
        let middleSection: any ComponentConvertible
        if let sidebar {
            let sidebarElements = sidebar.asRenderElements(in: context)
            middleSection = HStack(spacing: 0) {
                sidebarElements
                contentElements
            }
        } else {
            middleSection = contentElements
        }

        // Full vertical layout: [TopBar (optional) + Middle + BottomBar (optional)]
        let mainLayout = VStack(spacing: 0) {
            if let topBar {
                topBar.asRenderElements(in: context)
            }

            middleSection.asRenderElements(in: context)

            if let bottomBar {
                bottomBar.asRenderElements(in: context)
            }
        }

        // Stack with optional floating overlay
        if let overlay {
            return Stack {
                mainLayout
                overlay.asRenderElements(in: context)
            }
            .render(in: context)
        } else {
            return mainLayout.render(in: context)
        }
    }
}
