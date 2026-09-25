import Foundation

/// Constructs a `LayoutNode` hierarchy from a normalized `RenderElement` tree.
public enum LayoutTreeBuilder {
    /// Builds a `LayoutNode` tree corresponding to the given `RenderElement`.
    public static func build(from element: RenderElement) -> LayoutNode {
        let style = resolveLayoutStyle(from: element)
        let measurePolicy = resolveMeasurePolicy(from: element)
        let children = element.children.map { build(from: $0) }

        return LayoutNode(
            id: element.id,
            style: style,
            children: children,
            measurePolicy: measurePolicy
        )
    }

    private static func resolveLayoutStyle(from element: RenderElement) -> LayoutStyle {
        let resolved = element.resolvedStyle
        var style = LayoutStyle()

        // Dimensions
        if let w = resolved.width {
            style.width = .fixed(w)
        } else if case .icon = element.kind, let sizeStr = element.props.custom["iconSize"], let val = Double(sizeStr) {
            style.width = .fixed(val)
        }
        if let h = resolved.height {
            style.height = .fixed(h)
        } else if case .icon = element.kind, let sizeStr = element.props.custom["iconSize"], let val = Double(sizeStr) {
            style.height = .fixed(val)
        }
        style.minWidth = resolved.minWidth
        style.maxWidth = resolved.maxWidth
        style.minHeight = resolved.minHeight
        style.maxHeight = resolved.maxHeight
        style.padding = resolved.padding
        style.margin = resolved.margin
        style.zIndex = resolved.zIndex

        // Kind-specific flex defaults
        switch element.kind {
        case .stack(let axis, let alignment, let spacing):
            style.direction = axis == .vertical ? .column : .row
            switch alignment {
            case .start: style.alignItems = .start
            case .center: style.alignItems = .center
            case .end: style.alignItems = .end
            case .stretch: style.alignItems = .stretch
            }
            style.gap = spacing
            style.width = resolved.width.map { .fixed($0) } ?? .intrinsic
            style.height = resolved.height.map { .fixed($0) } ?? .intrinsic

        case .spacer(let minLength):
            style.flexGrow = 1.0
            if let min = minLength {
                style.minHeight = min
                style.minWidth = min
            }

        default:
            break
        }

        return style
    }

    private static func resolveMeasurePolicy(from element: RenderElement) -> MeasurePolicy? {
        switch element.kind {
        case .text(let text):
            // Measurement must use the same semantic font sizes as TextRenderer;
            // otherwise larger roles (for example `display`) overlap adjacent rows.
            let fontSize: Double
            switch element.props.custom["fontRole"] {
            case "heading": fontSize = 22
            case "display": fontSize = 28
            default: fontSize = 16
            }
            let lineLimit = element.props.custom["lineLimit"].flatMap { Int($0) }
            return TextMeasurePolicy(text: text, fontSize: fontSize, lineLimit: lineLimit)

        case .shape(let shapeKind):
            switch shapeKind {
            case .circle:
                return ShapeMeasurePolicy(shapeType: .circle, defaultDiameter: 40)
            case .rectangle:
                return ShapeMeasurePolicy(shapeType: .rectangle, defaultDiameter: 0)
            }

        case .spacer(let minLength):
            return SpacerMeasurePolicy(minLength: minLength ?? 0, axis: .vertical)

        case .icon:
            let defaultSize: Double = {
                if let sizeStr = element.props.custom["iconSize"], let val = Double(sizeStr) {
                    return val
                }
                return 20
            }()
            return ShapeMeasurePolicy(shapeType: .rectangle, defaultDiameter: defaultSize)

        case .textEditor(_, let multiline):
            let defaultH = multiline ? 80.0 : 36.0
            return ShapeMeasurePolicy(shapeType: .rectangle, defaultDiameter: defaultH)

        case .image:
            let defaultW = Double(element.props.custom["width"] ?? "0") ?? 40.0
            let defaultH = Double(element.props.custom["height"] ?? "0") ?? 40.0
            return ShapeMeasurePolicy(shapeType: .rectangle, defaultDiameter: max(defaultW, defaultH))

        case .stack, .group, .empty, .custom, .portal, .scrollArea:
            return nil
        }
    }
}
