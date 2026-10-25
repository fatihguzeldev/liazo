import Foundation
import LiazoCapture

@MainActor func measureCapture(_ arguments: [String]) async throws {
    guard arguments.count >= 2, arguments.count <= 6 else { throw CaptureError.invalidRequest }
    let count = arguments.count > 2 ? Int(arguments[2]) : 50
    let width = arguments.count > 3 ? Int(arguments[3]) : 1920
    let format = arguments.count > 4 ? ImageFormat(rawValue: arguments[4]) : .png
    let quality = arguments.count > 5 ? Int(arguments[5]) : 90
    guard let count, (20...200).contains(count), let width, (320...4096).contains(width),
          let format, let quality, (1...100).contains(quality) else {
        throw CaptureError.invalidRequest
    }
    var options = ScreenOptions()
    options.maxWidth = width
    options.format = format
    options.quality = Double(quality) / 100
    let screen = ScreenCapture()
    // Warm source discovery and the first native capture separately from steady-state samples.
    _ = try await screen.capture(sourceID: arguments[1], options: options)
    var capture: [UInt64] = [], encode: [UInt64] = [], total: [UInt64] = [], sizes: [Int] = []
    var fingerprints = Set<Data>()
    var dimensions: [Int] = []
    for _ in 0..<count {
        let started = DispatchTime.now().uptimeNanoseconds
        let image = try await screen.capture(sourceID: arguments[1], options: options) { timing in
            capture.append(timing.captureNS)
            encode.append(timing.encodeNS)
        }
        total.append(DispatchTime.now().uptimeNanoseconds - started)
        sizes.append(image.bytes?.count ?? 0)
        fingerprints.insert(image.fingerprint)
        dimensions = [image.width, image.height]
    }
    func milliseconds(_ values: [UInt64]) -> [String: Double] {
        let ordered = values.sorted()
        func rank(_ proportion: Double) -> Double {
            Double(ordered[Int(ceil(Double(ordered.count) * proportion)) - 1]) / 1_000_000
        }
        return ["p50": rank(0.50), "p95": rank(0.95)]
    }
    let report: [String: Any] = [
        "scope": "native screenshot and encoding of current source; no network or model",
        "samples": count,
        "warmup_samples": 1,
        "dimensions": dimensions,
        "format": format.rawValue,
        "quality": quality,
        "capture_ms": milliseconds(capture),
        "canonicalize_hash_encode_ms": milliseconds(encode),
        "total_ms": milliseconds(total),
        "min_image_bytes": sizes.min()!,
        "max_image_bytes": sizes.max()!,
        "distinct_pixel_fingerprints": fingerprints.count,
        "duplicate_filter": "disabled to measure full encoding",
        "readability": "not assessed"
    ]
    FileHandle.standardOutput.write(
        try JSONSerialization.data(withJSONObject: report, options: [.sortedKeys]) + Data([10])
    )
}
