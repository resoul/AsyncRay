#if canImport(UIKit)
import UIKit

/// Thin UIKit host view embedding a Prism Virtual Render Tree.
/// Provides safe area insets, display scale factor, trait updates, and attaches the root CALayer.
@MainActor
public final class HostUIView: UIView, PrismHost {
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

    /// Controls whether the mounted tree consumes all, some, or none of UIKit safe-area insets.
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
        engine.mount(in: self.layer)
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
        NotificationCenter.default.removeObserver(self)
        onTeardown?()
        engine.teardown()
    }

    public var colorScheme: ColorScheme {
        traitCollection.userInterfaceStyle == .dark ? .dark : .light
    }

    public var scaleFactor: Double {
        #if os(iOS) || os(tvOS)
        let screenScale = window?.screen.scale ?? UIScreen.main.scale
        return Double(screenScale)
        #else
        return 2.0
        #endif
    }

    public var safeAreaDirectionalInsets: DirectionalEdgeInsets {
        DirectionalEdgeInsets(
            top: Double(safeAreaInsets.top),
            leading: Double(safeAreaInsets.left),
            bottom: Double(safeAreaInsets.bottom),
            trailing: Double(safeAreaInsets.right)
        )
    }

    private func updateFromSystem() {
        engine.bounds = bounds
        engine.scaleFactor = scaleFactor
        engine.colorScheme = colorScheme
        engine.safeAreaInsets = safeAreaDirectionalInsets
    }

    public override func layoutSubviews() {
        super.layoutSubviews()
        let oldBounds = engine.bounds
        updateFromSystem()
        if oldBounds != bounds {
            onBoundsChange?(bounds)
        }
        engine.render()
    }

    public override func didMoveToWindow() {
        super.didMoveToWindow()
        if window != nil {
            updateFromSystem()
            engine.render()
            onMount?()
            NotificationCenter.default.addObserver(
                self,
                selector: #selector(keyboardWillChangeFrame(_:)),
                name: UIResponder.keyboardWillChangeFrameNotification,
                object: nil
            )
            NotificationCenter.default.addObserver(
                self,
                selector: #selector(keyboardWillHide(_:)),
                name: UIResponder.keyboardWillHideNotification,
                object: nil
            )
        } else {
            NotificationCenter.default.removeObserver(self)
            teardown()
        }
    }

    @objc private func keyboardWillChangeFrame(_ notification: Notification) {
        guard let userInfo = notification.userInfo,
              let endFrameValue = userInfo[UIResponder.keyboardFrameEndUserInfoKey] as? NSValue,
              let window = self.window else { return }
        let endFrame = window.convert(endFrameValue.cgRectValue, to: self)
        let intersection = bounds.intersection(endFrame)
        let keyboardHeight = intersection.isNull ? 0 : intersection.height
        var insets = safeAreaDirectionalInsets
        insets.bottom = max(insets.bottom, Double(keyboardHeight))
        engine.safeAreaInsets = insets
        engine.render()
    }

    @objc private func keyboardWillHide(_ notification: Notification) {
        engine.safeAreaInsets = safeAreaDirectionalInsets
        engine.render()
    }

    public override func traitCollectionDidChange(_ previousTraitCollection: UITraitCollection?) {
        super.traitCollectionDidChange(previousTraitCollection)
        if traitCollection.hasDifferentColorAppearance(comparedTo: previousTraitCollection) {
            let newScheme = colorScheme
            engine.colorScheme = newScheme
            onColorSchemeChange?(newScheme)
            engine.render()
        }
    }

    public override func safeAreaInsetsDidChange() {
        super.safeAreaInsetsDidChange()
        engine.safeAreaInsets = safeAreaDirectionalInsets
        engine.render()
    }

    public func findAccessibilityElement(byTestID testID: String) -> AccessibilityElement? {
        engine.accessibilityTree.findElement(byTestID: testID)
    }

    // MARK: - Native UIAccessibility Bridge

    public override var isAccessibilityElement: Bool {
        get { false }
        set { }
    }

    public override var accessibilityElements: [Any]? {
        get {
            engine.accessibilityTree.elements.values.map { ax -> UIAccessibilityElement in
                let elem = UIAccessibilityElement(accessibilityContainer: self)
                elem.accessibilityIdentifier = ax.testID
                elem.accessibilityLabel = ax.label ?? ax.testID
                elem.accessibilityValue = ax.value
                elem.accessibilityHint = ax.hint
                elem.accessibilityFrameInContainerSpace = ax.frame
                var traits: UIAccessibilityTraits = []
                if ax.traits.contains(.button) { traits.insert(.button) }
                if ax.traits.contains(.header) { traits.insert(.header) }
                if ax.traits.contains(.selected) { traits.insert(.selected) }
                if ax.traits.contains(.staticText) { traits.insert(.staticText) }
                if ax.traits.contains(.searchField) { traits.insert(.searchField) }
                elem.accessibilityTraits = traits
                return elem
            }
        }
        set { }
    }

    public override var canBecomeFirstResponder: Bool { true }

    // MARK: - Touch & Press Handling

    public override func touchesBegan(_ touches: Set<UITouch>, with event: UIEvent?) {
        super.touchesBegan(touches, with: event)
        guard let touch = touches.first else { return }
        let loc = touch.location(in: self)
        engine.dispatchPointerEvent(
            type: .pointerDown,
            location: loc,
            button: .primary,
            pointerType: .touch,
            modifiers: .none,
            clickCount: touch.tapCount
        )
    }

    public override func touchesMoved(_ touches: Set<UITouch>, with event: UIEvent?) {
        super.touchesMoved(touches, with: event)
        guard let touch = touches.first else { return }
        let loc = touch.location(in: self)
        let prevLoc = touch.previousLocation(in: self)
        let deltaX = Double(loc.x - prevLoc.x)
        let deltaY = Double(loc.y - prevLoc.y)

        engine.dispatchPointerEvent(
            type: .pointerMove,
            location: loc,
            button: .primary,
            pointerType: .touch,
            modifiers: .none,
            clickCount: touch.tapCount
        )

        if abs(deltaX) > 0.5 || abs(deltaY) > 0.5 {
            engine.dispatchScrollEvent(
                location: loc,
                deltaX: deltaX,
                deltaY: deltaY,
                phase: .changed
            )
        }
    }

    public override func touchesEnded(_ touches: Set<UITouch>, with event: UIEvent?) {
        super.touchesEnded(touches, with: event)
        guard let touch = touches.first else { return }
        let loc = touch.location(in: self)
        engine.dispatchPointerEvent(
            type: .pointerUp,
            location: loc,
            button: .primary,
            pointerType: .touch,
            modifiers: .none,
            clickCount: touch.tapCount
        )
        engine.dispatchScrollEvent(
            location: loc,
            deltaX: 0,
            deltaY: 0,
            phase: .ended
        )
    }

    public override func touchesCancelled(_ touches: Set<UITouch>, with event: UIEvent?) {
        super.touchesCancelled(touches, with: event)
        engine.eventDispatcher.reset()
        if let touch = touches.first {
            engine.dispatchScrollEvent(
                location: touch.location(in: self),
                deltaX: 0,
                deltaY: 0,
                phase: .cancelled
            )
        }
    }

    public override func pressesBegan(_ presses: Set<UIPress>, with event: UIPressesEvent?) {
        for press in presses {
            if let key = press.key {
                let characters = key.characters
                let charsIgnoring = key.charactersIgnoringModifiers
                let handled = engine.dispatchKeyEvent(
                    type: .keyDown,
                    key: characters,
                    characters: characters,
                    charactersIgnoringModifiers: charsIgnoring,
                    keyCode: UInt16(key.keyCode.rawValue),
                    modifiers: convertKeyModifierFlags(key.modifierFlags),
                    isRepeat: false
                )
                if handled == .handled {
                    return
                }
            }
        }
        super.pressesBegan(presses, with: event)
    }

    public override func pressesEnded(_ presses: Set<UIPress>, with event: UIPressesEvent?) {
        for press in presses {
            if let key = press.key {
                let characters = key.characters
                let charsIgnoring = key.charactersIgnoringModifiers
                engine.dispatchKeyEvent(
                    type: .keyUp,
                    key: characters,
                    characters: characters,
                    charactersIgnoringModifiers: charsIgnoring,
                    keyCode: UInt16(key.keyCode.rawValue),
                    modifiers: convertKeyModifierFlags(key.modifierFlags),
                    isRepeat: false
                )
            }
        }
        super.pressesEnded(presses, with: event)
    }

    private func convertKeyModifierFlags(_ flags: UIKeyModifierFlags) -> EventModifiers {
        var mods: EventModifiers = .none
        if flags.contains(.shift) { mods.insert(.shift) }
        if flags.contains(.control) { mods.insert(.control) }
        if flags.contains(.alternate) { mods.insert(.option) }
        if flags.contains(.command) { mods.insert(.command) }
        if flags.contains(.alphaShift) { mods.insert(.capsLock) }
        return mods
    }
}

extension HostUIView: UIKeyInput {
    public var hasText: Bool {
        !(PlatformTextInputAdapter.shared.activeDocument?.text.isEmpty ?? true)
    }

    public func insertText(_ text: String) {
        if let doc = PlatformTextInputAdapter.shared.activeDocument {
            doc.insert(text)
        } else {
            engine.dispatchKeyEvent(type: .keyDown, key: text, characters: text)
            engine.dispatchKeyEvent(type: .keyUp, key: text, characters: text)
        }
    }

    public func deleteBackward() {
        if let doc = PlatformTextInputAdapter.shared.activeDocument {
            doc.deleteBackward()
        } else {
            engine.dispatchKeyEvent(type: .keyDown, key: "\u{7F}", keyCode: 51)
            engine.dispatchKeyEvent(type: .keyUp, key: "\u{7F}", keyCode: 51)
        }
    }
}
#endif
