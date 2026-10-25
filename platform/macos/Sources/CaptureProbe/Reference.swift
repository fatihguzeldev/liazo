import CoreGraphics
import CoreText
import Foundation
import LiazoCapture

func writeReferenceImages(_ directory: URL) throws {
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    let width = 1920, height = 1080
    for (name, value) in [("a", 41), ("b", 42)] {
        guard let context = CGContext(
            data: nil, width: width, height: height, bitsPerComponent: 8, bytesPerRow: 0,
            space: CGColorSpace(name: CGColorSpace.sRGB)!,
            bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
        ) else { throw CaptureError.encodingFailed }
        context.setFillColor(CGColor(gray: 0.09, alpha: 1))
        context.fill(CGRect(x: 0, y: 0, width: width, height: height))
        // A deterministic textured panel prevents a text-only PNG from understating bandwidth.
        var noise: UInt32 = 0x12345678
        for y in 0..<height {
            for x in 1664..<width {
                noise ^= noise << 13; noise ^= noise >> 17; noise ^= noise << 5
                context.setFillColor(CGColor(gray: 0.12 + Double(noise & 15) / 255, alpha: 1))
                context.fill(CGRect(x: x, y: y, width: 1, height: 1))
            }
        }
        for (column, size) in [11.0, 13.0, 16.0, 20.0].enumerated() {
            let font = CTFontCreateWithName("Menlo" as CFString, size, nil)
            let lines = [
                "// liazo reference · \(Int(size)) px",
                "const total = \(value);",
                "const delta = total - previous;",
                "if (delta !== 0) publish(delta);",
                "return { source, epoch, total };",
                "Il1 O0 [] {} () <> != === :;",
                "küçük değişiklik / small edit"
            ]
            for block in 0..<5 {
                for (row, text) in lines.enumerated() {
                    let attributes: [NSAttributedString.Key: Any] = [
                        NSAttributedString.Key(kCTFontAttributeName as String): font,
                        NSAttributedString.Key(kCTForegroundColorAttributeName as String):
                            CGColor(gray: 0.93, alpha: 1)
                    ]
                    let line = CTLineCreateWithAttributedString(
                        NSAttributedString(string: text, attributes: attributes)
                    )
                    context.textPosition = CGPoint(x: 20 + column * 414,
                                                   y: 1030 - block * 200 - row * 24)
                    CTLineDraw(line, context)
                }
            }
        }
        guard let image = context.makeImage() else { throw CaptureError.encodingFailed }
        for format in [ImageFormat.png, .jpeg] {
            let encoded = try ImageEncoder.encode(image, format: format, quality: 0.9)
            guard let bytes = encoded.bytes else { throw CaptureError.encodingFailed }
            try bytes.write(to: directory.appendingPathComponent("\(name).\(format.rawValue)"),
                            options: .atomic)
        }
    }
    print("reference: 1920x1080; menlo 11/13/16/20px; total changes from 41 to 42; jpeg quality 90")
}
