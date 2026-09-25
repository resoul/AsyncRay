import Foundation

/// Modifiers applied to Virtual Render Tree elements.
public enum ElementModifier: Equatable, Sendable, CustomStringConvertible {
    case width(Double)
    case height(Double)
    case minWidth(Double)
    case maxWidth(Double)
    case minHeight(Double)
    case maxHeight(Double)
    case padding(DirectionalEdgeInsets)
    case margin(DirectionalEdgeInsets)
    case background(Color)
    /// Foreground color is kept separate from background so later surface fills cannot hide text.
    case foreground(Color)
    case opacity(Double)
    case zIndex(Int)
    case explicitKey(String)
    case testID(String)
    case transition(Transition)
    case animation(Animation?)
    case sdfRoundedRect(cornerRadius: Double, borderWidth: Double = 0, borderColor: Color = .clear, fill: Color? = nil)
    case glassmorphism(blurRadius: Double = 20, tint: Color = Color(red: 1, green: 1, blue: 1, alpha: 0.2), saturation: Double = 1.2)
    case meshGradient(MeshGradientGrid)

    public var description: String {
        switch self {
        case .width(let w): return "width(\(w))"
        case .height(let h): return "height(\(h))"
        case .minWidth(let w): return "minWidth(\(w))"
        case .maxWidth(let w): return "maxWidth(\(w))"
        case .minHeight(let h): return "minHeight(\(h))"
        case .maxHeight(let h): return "maxHeight(\(h))"
        case .padding(let insets):
            return "padding(t: \(insets.top), l: \(insets.leading), b: \(insets.bottom), tr: \(insets.trailing))"
        case .margin(let insets):
            return "margin(t: \(insets.top), l: \(insets.leading), b: \(insets.bottom), tr: \(insets.trailing))"
        case .background(let color): return "background(\(color))"
        case .foreground(let color): return "foreground(\(color))"
        case .opacity(let o): return "opacity(\(o))"
        case .zIndex(let z): return "zIndex(\(z))"
        case .explicitKey(let k): return "key(\"\(k)\")"
        case .testID(let id): return "testID(\"\(id)\")"
        case .transition(let t): return "transition(\(t))"
        case .animation(let a): return "animation(\(String(describing: a)))"
        case .sdfRoundedRect(let r, let bw, let bc, let f):
            return "sdfRoundedRect(r: \(r), bw: \(bw), bc: \(bc), fill: \(String(describing: f)))"
        case .glassmorphism(let br, let t, let s):
            return "glassmorphism(r: \(br), tint: \(t), sat: \(s))"
        case .meshGradient(let grid):
            return "meshGradient(\(grid.width)x\(grid.height))"
        }
    }
}

/// Resolved parameters for Signed Distance Field rounded rect rendering.
public struct SDFRoundedRectStyle: Equatable, Sendable {
    public var cornerRadius: Double
    public var borderWidth: Double
    public var borderColor: Color
    public var fill: Color?

    public init(cornerRadius: Double, borderWidth: Double = 0, borderColor: Color = .clear, fill: Color? = nil) {
        self.cornerRadius = cornerRadius
        self.borderWidth = borderWidth
        self.borderColor = borderColor
        self.fill = fill
    }
}

/// Resolved parameters for glassmorphism / frosted blur rendering.
public struct GlassmorphismStyle: Equatable, Sendable {
    public var blurRadius: Double
    public var tint: Color
    public var saturation: Double

    public init(blurRadius: Double = 20, tint: Color = Color(red: 1, green: 1, blue: 1, alpha: 0.2), saturation: Double = 1.2) {
        self.blurRadius = blurRadius
        self.tint = tint
        self.saturation = saturation
    }
}


/// Resolved layout and visual style folded from an array of ElementModifiers.
public struct ResolvedStyle: Equatable, Sendable, CustomStringConvertible {
    public var width: Double?
    public var height: Double?
    public var minWidth: Double?
    public var maxWidth: Double?
    public var minHeight: Double?
    public var maxHeight: Double?
    public var padding: DirectionalEdgeInsets
    public var margin: DirectionalEdgeInsets
    public var background: Color?
    public var foreground: Color?
    public var opacity: Double
    public var zIndex: Int
    public var sdfRoundedRect: SDFRoundedRectStyle?
    public var glassmorphism: GlassmorphismStyle?
    public var meshGradient: MeshGradientGrid?

    public init(
        width: Double? = nil,
        height: Double? = nil,
        minWidth: Double? = nil,
        maxWidth: Double? = nil,
        minHeight: Double? = nil,
        maxHeight: Double? = nil,
        padding: DirectionalEdgeInsets = .zero,
        margin: DirectionalEdgeInsets = .zero,
        background: Color? = nil,
        foreground: Color? = nil,
        opacity: Double = 1.0,
        zIndex: Int = 0,
        sdfRoundedRect: SDFRoundedRectStyle? = nil,
        glassmorphism: GlassmorphismStyle? = nil,
        meshGradient: MeshGradientGrid? = nil
    ) {
        self.width = width
        self.height = height
        self.minWidth = minWidth
        self.maxWidth = maxWidth
        self.minHeight = minHeight
        self.maxHeight = maxHeight
        self.padding = padding
        self.margin = margin
        self.background = background
        self.foreground = foreground
        self.opacity = opacity
        self.zIndex = zIndex
        self.sdfRoundedRect = sdfRoundedRect
        self.glassmorphism = glassmorphism
        self.meshGradient = meshGradient
    }

    /// Resolves an array of modifiers using deterministic precedence rules:
    /// - Dimensions: later overrides earlier.
    /// - Padding: accumulates (outer + inner).
    /// - Margin: accumulates (outer + inner).
    /// - Opacity: multiplicative (outer * inner, clamped to [0.0, 1.0]).
    /// - Background: later overrides earlier.
    /// - zIndex: later overrides earlier.
    /// - Effects: later overrides earlier.
    public static func resolve(from modifiers: [ElementModifier]) -> ResolvedStyle {
        var style = ResolvedStyle()

        for modifier in modifiers {
            switch modifier {
            case .width(let w):
                style.width = w
            case .height(let h):
                style.height = h
            case .minWidth(let w):
                style.minWidth = w
            case .maxWidth(let w):
                style.maxWidth = w
            case .minHeight(let h):
                style.minHeight = h
            case .maxHeight(let h):
                style.maxHeight = h
            case .padding(let insets):
                style.padding = DirectionalEdgeInsets(
                    top: style.padding.top + insets.top,
                    leading: style.padding.leading + insets.leading,
                    bottom: style.padding.bottom + insets.bottom,
                    trailing: style.padding.trailing + insets.trailing
                )
            case .margin(let insets):
                style.margin = DirectionalEdgeInsets(
                    top: style.margin.top + insets.top,
                    leading: style.margin.leading + insets.leading,
                    bottom: style.margin.bottom + insets.bottom,
                    trailing: style.margin.trailing + insets.trailing
                )
            case .background(let color):
                style.background = color
            case .foreground(let color):
                style.foreground = color
            case .opacity(let o):
                style.opacity = max(0.0, min(1.0, style.opacity * o))
            case .zIndex(let z):
                style.zIndex = z
            case .sdfRoundedRect(let r, let bw, let bc, let f):
                style.sdfRoundedRect = SDFRoundedRectStyle(cornerRadius: r, borderWidth: bw, borderColor: bc, fill: f)
            case .glassmorphism(let br, let t, let s):
                style.glassmorphism = GlassmorphismStyle(blurRadius: br, tint: t, saturation: s)
            case .meshGradient(let grid):
                style.meshGradient = grid
            case .explicitKey, .testID, .transition, .animation:
                break
            }
        }

        return style
    }

    public var description: String {
        var parts: [String] = []
        if let width { parts.append("width: \(width)") }
        if let height { parts.append("height: \(height)") }
        if padding != .zero { parts.append("padding: \(padding)") }
        if margin != .zero { parts.append("margin: \(margin)") }
        if let background { parts.append("background: \(background)") }
        if let foreground { parts.append("foreground: \(foreground)") }
        if opacity < 1.0 { parts.append("opacity: \(opacity)") }
        if zIndex != 0 { parts.append("zIndex: \(zIndex)") }
        if let sdf = sdfRoundedRect { parts.append("sdfRoundedRect(r: \(sdf.cornerRadius))") }
        if let glass = glassmorphism { parts.append("glass(r: \(glass.blurRadius))") }
        if let mesh = meshGradient { parts.append("meshGradient(\(mesh.width)x\(mesh.height))") }
        return parts.joined(separator: ", ")
    }
}
