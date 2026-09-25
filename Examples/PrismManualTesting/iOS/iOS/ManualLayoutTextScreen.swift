import PrismUI

struct ManualLayoutTextScreen: Component {
    func body(context: ComponentContext) -> RenderElement {
        let colors = context.theme?.colors ?? Theme.fallbackDefault().colors
        return VStack(alignment: .stretch, spacing: 20) {
            Text("Prism manual testing").font(.heading).foregroundColor(colors.foreground)
            Text("Layout + Text").font(.display).foregroundColor(colors.primary)
            Text("Resize the window or rotate the device. Inspect spacing, alignment, wrapping, and sizing.").foregroundColor(colors.mutedForeground).lineLimit(nil)
            Divider(color: colors.border)
            HStack(alignment: .center, spacing: 12) {
                Text("Leading").foregroundColor(colors.foreground).frame(width: 100, height: 44).background(colors.secondary)
                Spacer()
                Text("Flexible spacer").foregroundColor(colors.mutedForeground)
                Spacer(minLength: 8)
                Text("Trailing").foregroundColor(colors.foreground).frame(width: 100, height: 44).background(colors.accent)
            }
            Text("A deliberately long line demonstrates text measurement and wrapping inside the available width.").foregroundColor(colors.foreground).padding(16).background(colors.muted)
        }.padding(24).background(colors.background).render(in: context)
    }
}
