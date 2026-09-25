import Foundation
import QuartzCore
import CoreText

/// Renderer for text nodes using CATextLayer and CoreText typography.
@MainActor
public final class TextRenderer: LayerRenderer {
    public let elementID: ElementID
    public let textLayer: CATextLayer

    public var rootLayer: CALayer { textLayer }

    public init(elementID: ElementID) {
        self.elementID = elementID
        let layer = CATextLayer()
        layer.name = "TextLayer[\(elementID)]"
        layer.isWrapped = true
        layer.truncationMode = .end
        self.textLayer = layer
    }

    public func update(element: RenderElement, frame: LayoutFrame, context: RenderContext) {
        RenderTransaction.perform(disableActions: context.disableActions) {
            textLayer.bounds = CGRect(x: 0, y: 0, width: frame.width, height: frame.height)
            textLayer.position = CGPoint(
                x: frame.origin.x + frame.width / 2.0,
                y: frame.origin.y + frame.height / 2.0
            )

            // Ensure Retina display sharpness
            textLayer.contentsScale = CGFloat(context.scaleFactor)

            let textContent: String
            switch element.kind {
            case .text(let str):
                textContent = str
            default:
                textContent = ""
            }

            // Resolve typography attributes
            let fontRoleDesc = element.props.custom["fontRole"]
            let alignmentStr = element.props.custom["alignment"]

            // Alignment mapping
            switch alignmentStr {
            case "center":
                textLayer.alignmentMode = .center
            case "trailing":
                textLayer.alignmentMode = .right
            default:
                textLayer.alignmentMode = .left
            }

            let style = element.resolvedStyle
            // Foreground and surface are independent; a card/background must not recolor its text.
            let textColor = style.foreground ?? Color.black

            let ctFont: CTFont
            let fontSize: CGFloat = fontRoleDesc == "heading" ? 22 : (fontRoleDesc == "display" ? 28 : 16)
            ctFont = CTFontCreateUIFontForLanguage(.system, fontSize, nil)
                ?? CTFontCreateWithName("Helvetica" as CFString, fontSize, nil)

            let attributes: [NSAttributedString.Key: Any] = [
                NSAttributedString.Key(kCTFontAttributeName as String): ctFont,
                NSAttributedString.Key(kCTForegroundColorAttributeName as String): textColor.cgColor
            ]

            textLayer.string = NSAttributedString(string: textContent, attributes: attributes)
            textLayer.opacity = Float(style.opacity)
            textLayer.zPosition = CGFloat(style.zIndex)
        }
    }

    public func destroy() {
        textLayer.removeFromSuperlayer()
    }
}
