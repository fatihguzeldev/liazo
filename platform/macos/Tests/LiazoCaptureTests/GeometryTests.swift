import Foundation
import CoreGraphics
import LiazoCapture

struct CheckFailure: Error { let message: String }

func expect(_ condition: @autoclosure () throws -> Bool, _ message: String) throws {
    if try !condition() { throw CheckFailure(message: message) }
}

func rejects(_ operation: () throws -> Void, _ message: String) throws {
    do { try operation() } catch { return }
    throw CheckFailure(message: message)
}

@main struct NativeTests {
    static func main() throws {
        let fitted = try CaptureGeometry.fit(width: 3024, height: 1964, maxWidth: 1512)
        try expect(fitted.width == 1512 && fitted.height == 982, "retina aspect ratio changed")
        let original = try CaptureGeometry.fit(width: 800, height: 600, maxWidth: 1200)
        try expect(original.width == 800 && original.height == 600, "small source was upscaled")
        for (width, height) in [(0, 100), (100, -1), (Int.max, 100), (10000, 10000)] {
            try rejects({ _ = try CaptureGeometry.fit(width: width, height: height) },
                        "invalid geometry accepted before allocation")
        }
        let source = CGRect(x: 0, y: 0, width: 1920, height: 1080)
        for crop in [
            CGRect(x: -1, y: 0, width: 400, height: 200),
            CGRect(x: 1800, y: 0, width: 400, height: 200),
            CGRect(x: 0, y: 0, width: Double.nan, height: 200)
        ] {
            try rejects({ try CaptureGeometry.validateCrop(crop, within: source) },
                        "crop escaped selected source")
        }
        try CaptureGeometry.validateCrop(CGRect(x: 10, y: 20, width: 400, height: 200),
                                         within: source)
        print("geometry: aspect, limits, and crop checks passed")
        try checkImages()
    }
}

func checkImages() throws {
    func image(_ bytes: [UInt8]) throws -> CGImage {
        let provider = CGDataProvider(data: Data(bytes) as CFData)!
        return CGImage(width: 2, height: 1, bitsPerComponent: 8, bitsPerPixel: 32,
                       bytesPerRow: 8, space: CGColorSpace(name: CGColorSpace.sRGB)!,
                       bitmapInfo: CGBitmapInfo(rawValue: CGImageAlphaInfo.premultipliedLast.rawValue),
                       provider: provider, decode: nil, shouldInterpolate: false,
                       intent: .defaultIntent)!
    }
    let first = try ImageEncoder.encode(image([255, 0, 0, 255, 0, 0, 0, 255]))
    try expect(first.bytes?.prefix(8) == Data([137, 80, 78, 71, 13, 10, 26, 10]), "invalid png output")
    let same = try ImageEncoder.encode(image([255, 0, 0, 255, 0, 0, 0, 255]),
                                       ifDifferentFrom: first.fingerprint)
    try expect(same.bytes == nil, "identical pixels were re-encoded")
    let changed = try ImageEncoder.encode(image([255, 0, 0, 255, 1, 0, 0, 255]),
                                          ifDifferentFrom: first.fingerprint)
    try expect(changed.bytes != nil && changed.fingerprint != first.fingerprint,
               "one-channel edit disappeared")
    print("images: png, exact duplicates, and one-channel changes passed")
}
