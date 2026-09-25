import AppKit
import PrismUI

@main
@MainActor
final class ManualAppDelegate: NSObject, NSApplicationDelegate {
    private var window: NSWindow?
    func applicationDidFinishLaunching(_ notification: Notification) {
        let theme = Theme.fallbackDefault()
        let host = HostNSView(element: ManualLayoutTextScreen().render(in: ComponentContext(theme: theme)), theme: theme)
        host.safeAreaPolicy = .all
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 900, height: 620), styleMask: [.titled, .closable, .resizable], backing: .buffered, defer: false)
        window.title = "Prism Manual Layout + Text"
        window.contentView = host
        window.center(); window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
        self.window = window
    }
    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool { true }
}
