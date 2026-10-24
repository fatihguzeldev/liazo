import Foundation
import CoreGraphics

public enum CaptureError: Error, LocalizedError {
    case invalidGeometry
    case invalidCrop
    case permissionRequired
    case sourceMissing
    case encodingFailed
    case invalidRequest

    public var errorDescription: String? {
        switch self {
        case .invalidGeometry: "image dimensions exceed the capture limits"
        case .invalidCrop: "crop must fit inside the selected source"
        case .permissionRequired: "allow screen recording for liazo in system settings"
        case .sourceMissing: "source is no longer available"
        case .encodingFailed: "could not encode the captured image"
        case .invalidRequest: "invalid capture request"
        }
    }
}

public struct CaptureGeometry: Sendable {
    public let width: Int
    public let height: Int

    public static func fit(width: Int, height: Int, maxWidth: Int? = nil) throws -> Self {
        guard width > 0, height > 0, width <= 65536, height <= 65536 else {
            throw CaptureError.invalidGeometry
        }
        if let maxWidth, maxWidth <= 0 { throw CaptureError.invalidGeometry }
        let scale = min(1, Double(maxWidth ?? width) / Double(width))
        let outputWidth = max(1, Int(Double(width) * scale))
        let outputHeight = max(1, Int(Double(height) * scale))
        guard outputWidth <= 16_777_216 / outputHeight else {
            throw CaptureError.invalidGeometry
        }
        return Self(width: outputWidth, height: outputHeight)
    }

    public static func validateCrop(_ crop: CGRect, within bounds: CGRect) throws {
        guard [crop.minX, crop.minY, crop.width, crop.height].allSatisfy(\.isFinite),
              crop.width > 0, crop.height > 0, bounds.contains(crop) else {
            throw CaptureError.invalidCrop
        }
    }
}
