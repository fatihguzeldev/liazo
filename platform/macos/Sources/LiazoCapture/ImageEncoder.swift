import CoreGraphics
import CryptoKit
import Foundation
import ImageIO
import UniformTypeIdentifiers

public enum ImageFormat: String, Codable, Sendable {
    case png, jpeg
    public var mimeType: String { self == .png ? "image/png" : "image/jpeg" }
}

public struct EncodedImage: Sendable {
    public let width: Int
    public let height: Int
    public let fingerprint: Data
    public let bytes: Data?
    public let format: ImageFormat
}

public enum ImageEncoder {
    public static func encode(_ image: CGImage, format: ImageFormat = .png,
                              quality: Double = 0.9,
                              ifDifferentFrom previous: Data? = nil) throws -> EncodedImage {
        _ = try CaptureGeometry.fit(width: image.width, height: image.height)
        guard quality.isFinite, (0...1).contains(quality) else {
            throw CaptureError.invalidRequest
        }
        var pixels = Data(count: image.width * image.height * 4)
        try pixels.withUnsafeMutableBytes { buffer in
            guard let context = CGContext(
                data: buffer.baseAddress, width: image.width, height: image.height,
                bitsPerComponent: 8, bytesPerRow: image.width * 4,
                space: CGColorSpace(name: CGColorSpace.sRGB)!,
                bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue |
                            CGBitmapInfo.byteOrder32Big.rawValue
            ) else { throw CaptureError.encodingFailed }
            context.draw(image, in: CGRect(x: 0, y: 0, width: image.width, height: image.height))
        }
        var digest = SHA256()
        var dimensions = [UInt64(image.width).littleEndian, UInt64(image.height).littleEndian]
        dimensions.withUnsafeMutableBytes { digest.update(bufferPointer: UnsafeRawBufferPointer($0)) }
        digest.update(data: pixels)
        let fingerprint = Data(digest.finalize())
        var bytes: Data?
        if fingerprint != previous {
            let encoded = NSMutableData()
            let type = format == .png ? UTType.png.identifier : UTType.jpeg.identifier
            guard let destination = CGImageDestinationCreateWithData(encoded, type as CFString, 1, nil) else {
                throw CaptureError.encodingFailed
            }
            let properties = [kCGImageDestinationLossyCompressionQuality: quality] as CFDictionary
            CGImageDestinationAddImage(destination, image, properties)
            guard CGImageDestinationFinalize(destination), encoded.length <= 32 * 1024 * 1024 else {
                throw CaptureError.encodingFailed
            }
            bytes = encoded as Data
        }
        return EncodedImage(width: image.width, height: image.height,
                            fingerprint: fingerprint, bytes: bytes, format: format)
    }
}
