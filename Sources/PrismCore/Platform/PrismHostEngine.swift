import Foundation
import QuartzCore

/// Platform-agnostic host engine driving layout passes, CALayer tree synchronization,
/// and debug inspector overlays for a mounted Prism component hierarchy.
@MainActor
public final class PrismHostEngine {
    public var rootElement: RenderElement {
        didSet {
            render()
        }
    }

    public var scaleFactor: Double = 2.0 {
        didSet {
            if oldValue != scaleFactor { render() }
        }
    }

    public var colorScheme: ColorScheme = .light {
        didSet {
            if oldValue != colorScheme { render() }
        }
    }

    public var theme: Theme? = nil {
        didSet {
            if oldValue != theme { render() }
        }
    }

    public var safeAreaInsets: DirectionalEdgeInsets = .zero {
        didSet {
            if oldValue != safeAreaInsets { render() }
        }
    }

    /// Selectively applies platform safe-area edges while retaining the raw insets for diagnostics.
    public var safeAreaPolicy: SafeAreaPolicy = .all {
        didSet { if oldValue != safeAreaPolicy { render() } }
    }

    public var bounds: CGRect = .zero {
        didSet {
            if oldValue != bounds {
                overlayHost.updateBounds(bounds)
                render()
            }
        }
    }

    public var isInspectorOverlayEnabled: Bool = false {
        didSet {
            updateInspectorOverlay()
        }
    }

    public private(set) var rootRenderer: ContainerRenderer
    public private(set) var rootMountedNode: MountedNode
    public let eventDispatcher: EventDispatcher = EventDispatcher()
    public let focusTree: FocusTree = FocusTree()
    public let accessibilityTree: AccessibilityTree = AccessibilityTree()
    public let shortcutRegistry: KeyboardShortcutRegistry = KeyboardShortcutRegistry()
    public let overlayHost: OverlayHost = OverlayHost()
    public let anchorRegistry: AnchorRegistry = AnchorRegistry()

    public private(set) var rootLayoutNode: LayoutNode?
    public private(set) weak var hostLayer: CALayer?
    public private(set) var inspectorLayer: CALayer = CALayer()

    public convenience init(rootElement: RenderElement) {
        self.init(rootElement: rootElement, theme: nil)
    }

    public init(rootElement: RenderElement, theme: Theme?) {
        let normalized = rootElement.normalized()
        self.rootElement = normalized
        self.theme = theme
        let renderer = ContainerRenderer(elementID: normalized.id)
        self.rootRenderer = renderer
        self.rootMountedNode = MountedNode(element: normalized, renderer: renderer)
        self.inspectorLayer.name = "PrismInspectorOverlay"
        self.focusTree.rootNode = rootMountedNode
        self.overlayHost.engine = self
        self.rootMountedNode.overlayHost = self.overlayHost
    }

    /// Updates the active theme and triggers an immediate re-render.
    public func setTheme(_ theme: Theme?) {
        self.theme = theme
    }

    /// Attaches the Prism root CALayer into the host platform view's backing layer.
    public func mount(in containerLayer: CALayer) {
        self.hostLayer = containerLayer
        rootMountedNode.mount(superlayer: containerLayer, overlayHost: overlayHost)
        focusTree.rootNode = rootMountedNode
        render()
    }

    /// Executes the two-pass layout calculation (measure -> layout) and updates CALayer renderers.
    public func render() {
        guard bounds.width > 0, bounds.height > 0 else { return }

        let normalized = rootElement.normalized()
        let layoutNode = LayoutTreeBuilder.build(from: normalized)
        self.rootLayoutNode = layoutNode

        let effectiveInsets = safeAreaInsets.applying(safeAreaPolicy)
        let contentWidth = max(0, bounds.width - effectiveInsets.leading - effectiveInsets.trailing)
        let contentHeight = max(0, bounds.height - effectiveInsets.top - effectiveInsets.bottom)

        let constraint = SizeConstraint(
            width: .exactly(contentWidth),
            height: .atMost(contentHeight)
        )
        layoutNode.measure(constraint: constraint)

        let rootFrame = LayoutFrame(
            x: effectiveInsets.leading,
            y: effectiveInsets.top,
            width: contentWidth,
            height: contentHeight
        )

        let roundingPolicy = PixelRoundingPolicy(scaleFactor: scaleFactor)
        layoutNode.layout(frame: rootFrame, roundingPolicy: roundingPolicy)

        let renderContext = RenderContext(
            scaleFactor: scaleFactor,
            theme: theme,
            colorScheme: colorScheme,
            disableActions: true
        )

        rootRenderer.update(element: normalized, frame: rootFrame, context: renderContext)
        syncSubtree(containerRenderer: rootRenderer, element: normalized, layoutNode: layoutNode, context: renderContext)

        // Reconcile mounted node tree
        rootMountedNode.element = normalized
        Reconciler.reconcileTree(
            rootNode: rootMountedNode,
            newRootElement: normalized,
            bounds: bounds,
            safeArea: safeAreaInsets,
            context: renderContext
        )

        // Synchronize FocusTree and AccessibilityTree
        focusTree.rootNode = rootMountedNode
        accessibilityTree.update(from: rootMountedNode)

        // Synchronize AnchorRegistry and Overlays
        anchorRegistry.update(from: rootMountedNode)
        overlayHost.updateBounds(bounds)
        overlayHost.updateOverlayPositions()

        updateInspectorOverlay()
    }

    private func syncSubtree(
        containerRenderer: ContainerRenderer,
        element: RenderElement,
        layoutNode: LayoutNode,
        context: RenderContext
    ) {
        var childPairs: [(element: RenderElement, frame: LayoutFrame)] = []
        for (childElement, childLayout) in zip(element.children, layoutNode.children) {
            let frame = childLayout.layoutFrame ?? .zero
            childPairs.append((childElement, frame))
        }

        containerRenderer.updateChildren(children: childPairs, context: context)

        for (childElement, childLayout) in zip(element.children, layoutNode.children) {
            if let childContainer = containerRenderer.childRenderers[childElement.id] as? ContainerRenderer {
                syncSubtree(
                    containerRenderer: childContainer,
                    element: childElement,
                    layoutNode: childLayout,
                    context: context
                )
            }
        }
    }

    /// Detaches layers, unmounts mounted tree, resets event states, and destroys renderers cleanly.
    public func teardown() {
        overlayHost.teardown()
        inspectorLayer.removeFromSuperlayer()
        inspectorLayer.sublayers?.removeAll()
        rootRenderer.destroy()
        rootMountedNode.unmount()
        focusTree.setFocus(to: nil)
        focusTree.rootNode = nil
        eventDispatcher.reset()
        shortcutRegistry.reset()
        hostLayer = nil
    }

    // MARK: - Event Dispatching

    /// Dispatches a pointer event (touch/mouse/pen) into the mounted hierarchy.
    @discardableResult
    public func dispatchPointerEvent(
        type: EventType,
        location: CGPoint,
        button: PointerButton = .primary,
        pointerType: PointerType = .mouse,
        modifiers: EventModifiers = .none,
        clickCount: Int = 1
    ) -> EventResult {
        switch type {
        case .pointerMove:
            eventDispatcher.handlePointerMove(location: location, root: rootMountedNode, modifiers: modifiers)
            return .handled
        case .pointerDown:
            let hit = HitTester.hitTest(point: location, root: rootMountedNode, overlayHost: overlayHost)
            if hit == nil && overlayHost.handleBackdropTap() {
                return .handled
            }
            return eventDispatcher.handlePointerDown(
                location: location,
                root: rootMountedNode,
                button: button,
                pointerType: pointerType,
                modifiers: modifiers,
                clickCount: clickCount
            )
        case .pointerUp:
            return eventDispatcher.handlePointerUp(
                location: location,
                root: rootMountedNode,
                button: button,
                pointerType: pointerType,
                modifiers: modifiers,
                clickCount: clickCount
            )
        default:
            guard let hit = HitTester.hitTest(point: location, root: rootMountedNode, overlayHost: overlayHost) else { return .ignored }
            let localPoint = hit.convertToLocal(pointInHost: location)
            let data = PointerEventData(
                location: localPoint,
                globalLocation: location,
                button: button,
                pointerType: pointerType,
                modifiers: modifiers,
                clickCount: clickCount
            )
            let event = Event(type: type, targetID: hit.id, payload: .pointer(data))
            return eventDispatcher.dispatch(event: event, target: hit)
        }
    }

    /// Dispatches a key event, prioritizing shortcuts and Tab/Shift-Tab focus traversal.
    @discardableResult
    public func dispatchKeyEvent(
        type: EventType,
        key: String,
        characters: String = "",
        charactersIgnoringModifiers: String = "",
        keyCode: UInt16 = 0,
        modifiers: EventModifiers = .none,
        isRepeat: Bool = false
    ) -> EventResult {
        let keyData = KeyEventData(
            key: key,
            characters: characters,
            charactersIgnoringModifiers: charactersIgnoringModifiers,
            keyCode: keyCode,
            modifiers: modifiers,
            isRepeat: isRepeat
        )

        // 0. Escape key overlay dismissal
        if type == .keyDown && (key == "Escape" || keyCode == 53) {
            if overlayHost.handleEscapeKey() {
                return .handled
            }
        }

        let targetNode: MountedNode
        if let focusedID = focusTree.currentFocus,
           let focusedNode = findNode(by: focusedID) {
            targetNode = focusedNode
        } else {
            targetNode = rootMountedNode
        }

        let event = Event(type: type, targetID: targetNode.id, payload: .key(keyData))

        // 1. Shortcut registry execution on keyDown
        if type == .keyDown && shortcutRegistry.handleKeyEvent(event) {
            return .handled
        }

        // 2. Tab / Shift-Tab focus traversal
        if type == .keyDown && (keyData.key == "\t" || keyData.keyCode == 48) {
            let direction: FocusDirection = modifiers.contains(.shift) ? .previous : .next
            if focusTree.moveFocus(direction: direction) {
                event.preventDefault()
                event.stopPropagation()
                return .handled
            }
        }

        // 3. Space / Return activation for focused button/interactive element
        if type == .keyDown && (keyData.key == " " || keyData.key == "\r" || keyData.key == "\n" || keyData.keyCode == 49 || keyData.keyCode == 36) {
            let hasTap = targetNode.eventHandlers[.tap] != nil
                || ActionRegistry.shared.action(for: targetNode.id) != nil
                || targetNode.element.props.testID.flatMap({ ActionRegistry.shared.action(forTestID: $0) }) != nil
            if hasTap {
                let tapEvent = Event(
                    type: .tap,
                    targetID: targetNode.id,
                    payload: .pointer(PointerEventData(location: .zero, globalLocation: .zero))
                )
                let res = eventDispatcher.dispatch(event: tapEvent, target: targetNode)
                if let act = ActionRegistry.shared.action(for: targetNode.id)
                    ?? targetNode.element.props.testID.flatMap({ ActionRegistry.shared.action(forTestID: $0) }) {
                    act()
                }
                event.preventDefault()
                event.stopPropagation()
                return res == .handled ? .handled : .handled
            }
        }

        return eventDispatcher.dispatch(event: event, target: targetNode)
    }

    /// Dispatches a scroll event into the mounted hierarchy at the given location.
    @discardableResult
    public func dispatchScrollEvent(
        location: CGPoint,
        deltaX: Double,
        deltaY: Double,
        phase: ScrollPhase = .changed,
        modifiers: EventModifiers = .none
    ) -> EventResult {
        overlayHost.updateOverlayPositions()
        guard let hit = HitTester.hitTest(point: location, root: rootMountedNode, overlayHost: overlayHost) else { return .ignored }
        let scrollData = ScrollEventData(
            location: hit.convertToLocal(pointInHost: location),
            deltaX: deltaX,
            deltaY: deltaY,
            phase: phase,
            modifiers: modifiers
        )
        let event = Event(type: .scroll, targetID: hit.id, payload: .scroll(scrollData))
        return eventDispatcher.dispatch(event: event, target: hit)
    }

    /// Recursively locates a mounted node by its element identifier in either the content tree or active overlays.
    public func findNode(by id: ElementID) -> MountedNode? {
        if let match = findNode(by: id, in: rootMountedNode) {
            return match
        }
        for entry in overlayHost.activeEntries.values {
            if let match = findNode(by: id, in: entry.node) {
                return match
            }
        }
        return nil
    }

    /// Returns the active mounted node for a given anchor identifier.
    public func findNodeByAnchor(_ anchorID: String) -> MountedNode? {
        anchorRegistry.node(for: anchorID)
    }

    /// Recomputes positions for all active anchored overlays (e.g. after dynamic scroll or resize).
    public func invalidateOverlayPositions() {
        anchorRegistry.update(from: rootMountedNode)
        overlayHost.updateBounds(bounds)
        overlayHost.updateOverlayPositions()
    }

    /// Development diagnostics for duplicate testIDs in the tree.
    public var testIDConflicts: [TestIDConflict] {
        TestIDValidator.findConflicts(in: rootMountedNode)
    }

    private func findNode(by id: ElementID, in root: MountedNode) -> MountedNode? {
        if root.id == id { return root }
        for child in root.children {
            if let match = findNode(by: id, in: child) { return match }
        }
        return nil
    }

    /// Returns a structured diagnostic dump of the host state, element tree, layout trace, and layer tree.
    public func dumpDiagnostics() -> String {
        var output = "=== Prism Host Diagnostics ===\n"
        output += "Bounds: \(bounds)\n"
        output += "Scale: \(scaleFactor), Scheme: \(colorScheme), SafeArea: \(safeAreaInsets)\n"
        output += "--- Element Tree ---\n"
        output += rootElement.dumpTree() + "\n"
        if let node = rootLayoutNode {
            output += "--- Layout Trace ---\n"
            output += node.dumpTrace() + "\n"
        }
        output += "--- CALayer Tree ---\n"
        output += LayerDiagnostics.dumpLayerTree(rootRenderer.rootLayer) + "\n"
        output += "Total Layers: \(LayerDiagnostics.totalLayerCount(rootRenderer.rootLayer))\n"
        return output
    }

    private func updateInspectorOverlay() {
        guard isInspectorOverlayEnabled, let host = hostLayer else {
            inspectorLayer.removeFromSuperlayer()
            inspectorLayer.sublayers?.removeAll()
            return
        }

        inspectorLayer.frame = host.bounds
        if inspectorLayer.superlayer == nil {
            host.addSublayer(inspectorLayer)
        }

        inspectorLayer.sublayers?.removeAll()
        if let rootNode = rootLayoutNode {
            addDebugOutlines(node: rootNode, parentOrigin: .zero)
        }
    }

    private func addDebugOutlines(node: LayoutNode, parentOrigin: CGPoint) {
        guard let frame = node.layoutFrame else { return }

        let outline = CALayer()
        let globalOrigin = CGPoint(x: parentOrigin.x + frame.origin.x, y: parentOrigin.y + frame.origin.y)
        outline.frame = CGRect(x: globalOrigin.x, y: globalOrigin.y, width: frame.width, height: frame.height)
        outline.borderWidth = 1.0 / CGFloat(scaleFactor)
        outline.borderColor = CGColor(red: 0.0, green: 0.75, blue: 1.0, alpha: 0.6)
        inspectorLayer.addSublayer(outline)

        for child in node.children {
            addDebugOutlines(node: child, parentOrigin: globalOrigin)
        }
    }
}
