#if canImport(AppKit) && !targetEnvironment(macCatalyst)
import AppKit

/// Thin AppKit host view embedding a Prism Virtual Render Tree.
/// Backed by a CALayer and forwards frame, backing scale factor, and appearance changes.
@MainActor
public final class HostNSView: NSView, PrismHost {
    public let engine: PrismHostEngine

    public var isInspectorOverlayEnabled: Bool {
        get { engine.isInspectorOverlayEnabled }
        set { engine.isInspectorOverlayEnabled = newValue }
    }

    public var onMount: (() -> Void)?
    public var onBoundsChange: ((CGRect) -> Void)?
    public var onColorSchemeChange: ((ColorScheme) -> Void)?
    public var onTeardown: (() -> Void)?

    public var theme: Theme? {
        get { engine.theme }
        set { engine.theme = newValue }
    }

    /// Controls whether the mounted tree consumes all, some, or none of AppKit safe-area insets.
    public var safeAreaPolicy: SafeAreaPolicy {
        get { engine.safeAreaPolicy }
        set { engine.safeAreaPolicy = newValue }
    }

    public convenience init(element: RenderElement) {
        self.init(element: element, theme: nil)
    }

    public init(element: RenderElement, theme: Theme?) {
        self.engine = PrismHostEngine(rootElement: element, theme: theme)
        super.init(frame: .zero)
        setupHost()
    }

    public required init?(coder: NSCoder) {
        self.engine = PrismHostEngine(rootElement: RenderElement(id: ElementID(typeName: "Empty"), kind: .empty))
        super.init(coder: coder)
        setupHost()
    }

    private func setupHost() {
        self.wantsLayer = true
        guard let layer = self.layer else { return }
        engine.mount(in: layer)
        updateFromSystem()
    }

    public func setRootElement(_ element: RenderElement) {
        engine.rootElement = element
    }

    public func setTheme(_ theme: Theme?) {
        engine.setTheme(theme)
    }

    public func render() {
        updateFromSystem()
        engine.render()
    }

    public func teardown() {
        onTeardown?()
        engine.teardown()
    }

    public var colorScheme: ColorScheme {
        let match = effectiveAppearance.bestMatch(from: [.darkAqua, .aqua])
        return match == .darkAqua ? .dark : .light
    }

    public var scaleFactor: Double {
        Double(window?.backingScaleFactor ?? NSScreen.main?.backingScaleFactor ?? 2.0)
    }

    public var safeAreaDirectionalInsets: DirectionalEdgeInsets {
        if #available(macOS 11.0, *) {
            return DirectionalEdgeInsets(
                top: Double(safeAreaInsets.top),
                leading: Double(safeAreaInsets.left),
                bottom: Double(safeAreaInsets.bottom),
                trailing: Double(safeAreaInsets.right)
            )
        }
        return .zero
    }

    private func updateFromSystem() {
        engine.bounds = bounds
        engine.scaleFactor = scaleFactor
        engine.colorScheme = colorScheme
        engine.safeAreaInsets = safeAreaDirectionalInsets
    }

    public override func layout() {
        super.layout()
        let oldBounds = engine.bounds
        updateFromSystem()
        if oldBounds != bounds {
            onBoundsChange?(bounds)
        }
        engine.render()
    }

    public override func setFrameSize(_ newSize: NSSize) {
        let oldBounds = engine.bounds
        super.setFrameSize(newSize)
        updateFromSystem()
        if oldBounds != bounds {
            onBoundsChange?(bounds)
        }
        engine.render()
    }


    public override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        if window != nil {
            updateFromSystem()
            engine.render()
            onMount?()
        } else {
            teardown()
        }
    }

    public override func viewDidChangeEffectiveAppearance() {
        super.viewDidChangeEffectiveAppearance()
        let newScheme = colorScheme
        engine.colorScheme = newScheme
        onColorSchemeChange?(newScheme)
        engine.render()
    }

    public override var isFlipped: Bool { true }
    public override var acceptsFirstResponder: Bool { true }

    public func findAccessibilityElement(byTestID testID: String) -> AccessibilityElement? {
        engine.accessibilityTree.findElement(byTestID: testID)
    }

    // MARK: - Native NSAccessibility Bridge

    public override func isAccessibilityElement() -> Bool { false }

    public override func accessibilityChildren() -> [Any]? {
        engine.accessibilityTree.elements.values.map { ax -> NSAccessibilityElement in
            let elem = NSAccessibilityElement()
            elem.setAccessibilityParent(self)
            elem.setAccessibilityIdentifier(ax.testID ?? "")
            elem.setAccessibilityLabel(ax.label ?? ax.testID ?? "")
            if let val = ax.value {
                elem.setAccessibilityValue(val)
            }
            if ax.traits.contains(.button) {
                elem.setAccessibilityRole(.button)
            } else {
                elem.setAccessibilityRole(.staticText)
            }
            if let win = window {
                let rectInWindow = convert(ax.frame, to: nil)
                let rectOnScreen = win.convertToScreen(rectInWindow)
                elem.setAccessibilityFrame(rectOnScreen)
            } else {
                elem.setAccessibilityFrame(ax.frame)
            }
            return elem
        }
    }

    public override func accessibilityHitTest(_ point: NSPoint) -> Any? {
        guard let win = window else { return self }
        let windowPoint = win.convertPoint(fromScreen: point)
        let localPoint = convert(windowPoint, from: nil)
        for (_, ax) in engine.accessibilityTree.elements {
            if ax.frame.contains(localPoint) {
                let elem = NSAccessibilityElement()
                elem.setAccessibilityParent(self)
                elem.setAccessibilityIdentifier(ax.testID ?? "")
                elem.setAccessibilityLabel(ax.label ?? ax.testID ?? "")
                if let val = ax.value { elem.setAccessibilityValue(val) }
                elem.setAccessibilityRole(ax.traits.contains(.button) ? .button : .staticText)
                let rectInWindow = convert(ax.frame, to: nil)
                elem.setAccessibilityFrame(win.convertToScreen(rectInWindow))
                return elem
            }
        }
        return super.accessibilityHitTest(point)
    }

    // MARK: - Tracking Areas & Mouse Events

    private var trackingArea: NSTrackingArea?

    public override func updateTrackingAreas() {
        super.updateTrackingAreas()
        if let trackingArea {
            removeTrackingArea(trackingArea)
        }
        let options: NSTrackingArea.Options = [.activeInKeyWindow, .mouseMoved, .mouseEnteredAndExited, .inVisibleRect]
        let area = NSTrackingArea(rect: bounds, options: options, owner: self, userInfo: nil)
        addTrackingArea(area)
        self.trackingArea = area
    }

    public override func mouseDown(with event: NSEvent) {
        let loc = convert(event.locationInWindow, from: nil)
        engine.dispatchPointerEvent(
            type: .pointerDown,
            location: loc,
            button: .primary,
            pointerType: .mouse,
            modifiers: convertModifiers(event.modifierFlags),
            clickCount: event.clickCount
        )
    }

    public override func mouseUp(with event: NSEvent) {
        let loc = convert(event.locationInWindow, from: nil)
        engine.dispatchPointerEvent(
            type: .pointerUp,
            location: loc,
            button: .primary,
            pointerType: .mouse,
            modifiers: convertModifiers(event.modifierFlags),
            clickCount: event.clickCount
        )
    }

    public override func mouseMoved(with event: NSEvent) {
        let loc = convert(event.locationInWindow, from: nil)
        engine.dispatchPointerEvent(
            type: .pointerMove,
            location: loc,
            button: .none,
            pointerType: .mouse,
            modifiers: convertModifiers(event.modifierFlags)
        )
    }

    public override func mouseDragged(with event: NSEvent) {
        let loc = convert(event.locationInWindow, from: nil)
        engine.dispatchPointerEvent(
            type: .pointerMove,
            location: loc,
            button: .primary,
            pointerType: .mouse,
            modifiers: convertModifiers(event.modifierFlags)
        )
    }

    public override func scrollWheel(with event: NSEvent) {
        let loc = convert(event.locationInWindow, from: nil)
        let phase: ScrollPhase
        switch event.phase {
        case .began: phase = .began
        case .changed: phase = .changed
        case .ended: phase = .ended
        case .cancelled: phase = .cancelled
        default: phase = .changed
        }
        engine.dispatchScrollEvent(
            location: loc,
            deltaX: Double(event.scrollingDeltaX),
            deltaY: Double(event.scrollingDeltaY),
            phase: phase,
            modifiers: convertModifiers(event.modifierFlags)
        )
    }

    public override func keyDown(with event: NSEvent) {
        let characters = event.characters ?? ""
        let charsIgnoring = event.charactersIgnoringModifiers ?? ""
        let mods = convertModifiers(event.modifierFlags)

        if PlatformTextInputAdapter.shared.activeDocument != nil {
            let handled = PlatformTextInputAdapter.shared.handleKey(
                key: characters,
                keyCode: event.keyCode,
                modifiers: mods,
                isRepeat: event.isARepeat
            )
            if handled {
                engine.render()
                return
            }
        }

        let handled = engine.dispatchKeyEvent(
            type: .keyDown,
            key: characters,
            characters: characters,
            charactersIgnoringModifiers: charsIgnoring,
            keyCode: event.keyCode,
            modifiers: mods,
            isRepeat: event.isARepeat
        )
        if handled != .handled {
            super.keyDown(with: event)
        }
    }

    public override func keyUp(with event: NSEvent) {
        let characters = event.characters ?? ""
        let charsIgnoring = event.charactersIgnoringModifiers ?? ""
        engine.dispatchKeyEvent(
            type: .keyUp,
            key: characters,
            characters: characters,
            charactersIgnoringModifiers: charsIgnoring,
            keyCode: event.keyCode,
            modifiers: convertModifiers(event.modifierFlags),
            isRepeat: event.isARepeat
        )
    }

    private func convertModifiers(_ flags: NSEvent.ModifierFlags) -> EventModifiers {
        var mods: EventModifiers = .none
        if flags.contains(.shift) { mods.insert(.shift) }
        if flags.contains(.control) { mods.insert(.control) }
        if flags.contains(.option) { mods.insert(.option) }
        if flags.contains(.command) { mods.insert(.command) }
        if flags.contains(.capsLock) { mods.insert(.capsLock) }
        return mods
    }
}

extension HostNSView: NSTextInputClient {
    public func insertText(_ string: Any, replacementRange: NSRange) {
        let str: String = (string as? String) ?? (string as? NSAttributedString)?.string ?? ""
        PlatformTextInputAdapter.shared.insertText(str, replacementRange: replacementRange)
        engine.render()
    }

    public func setMarkedText(_ string: Any, selectedRange: NSRange, replacementRange: NSRange) {
        let str: String = (string as? String) ?? (string as? NSAttributedString)?.string ?? ""
        PlatformTextInputAdapter.shared.setMarkedText(str, selectedRange: selectedRange, replacementRange: replacementRange)
        engine.render()
    }

    public func unmarkText() {
        PlatformTextInputAdapter.shared.unmarkText()
        engine.render()
    }

    public func selectedRange() -> NSRange {
        PlatformTextInputAdapter.shared.selectedRange
    }

    public func markedRange() -> NSRange {
        PlatformTextInputAdapter.shared.markedRange
    }

    public func hasMarkedText() -> Bool {
        PlatformTextInputAdapter.shared.hasMarkedText
    }

    public func attributedSubstring(forProposedRange range: NSRange, actualRange: NSRangePointer?) -> NSAttributedString? {
        guard let doc = PlatformTextInputAdapter.shared.activeDocument else { return nil }
        let charRange = doc.charRange(for: range)
        let sub = doc.substring(for: charRange)
        actualRange?.pointee = range
        return NSAttributedString(string: sub)
    }

    public func validAttributesForMarkedText() -> [NSAttributedString.Key] {
        []
    }

    public func firstRect(forCharacterRange range: NSRange, actualRange: NSRangePointer?) -> NSRect {
        var r = NSRect(origin: .zero, size: NSSize(width: 10, height: 20))
        if let win = window {
            r = win.convertToScreen(convert(r, to: nil))
        }
        return r
    }

    public func characterIndex(for point: NSPoint) -> Int {
        0
    }
}
#endif
