import AVFoundation
import AppKit
import CoreGraphics
import Foundation
@preconcurrency import ScreenCaptureKit

public struct ScreenSource: Codable, Sendable {
    public let id: String
    public let instance: String
    public let kind: String
    public let title: String
    public let width: Int
    public let height: Int
    public let scale: Double
    public let processID: Int32?
}

public struct ScreenOptions: Sendable {
    public var maxWidth: Int?
    public var cropPixels: CGRect?
    public var format: ImageFormat = .png
    public var quality: Double = 0.9
    public var showsCursor = false

    public init() {}
}

public struct CaptureTiming: Sendable {
    public let captureNS: UInt64
    public let encodeNS: UInt64
}

@MainActor public final class ScreenCapture {
    private var content: SCShareableContent?

    public init() {
        _ = NSScreen.screens
    }

    public static var hasPermission: Bool { CGPreflightScreenCaptureAccess() }

    public func sources() async throws -> [ScreenSource] {
        let content = try await refresh()
        let displays = content.displays.map { display in
            let instance = NativeIdentity.display(display.displayID)
            let filter = SCContentFilter(display: display, excludingWindows: [])
            return ScreenSource(
                id: "display:\(display.displayID)", instance: instance, kind: "display",
                title: "display \(display.displayID)",
                width: Int(filter.contentRect.width * CGFloat(filter.pointPixelScale)),
                height: Int(filter.contentRect.height * CGFloat(filter.pointPixelScale)),
                scale: Double(filter.pointPixelScale), processID: nil
            )
        }
        let windows = content.windows.filter { $0.windowLayer == 0 && $0.frame.width > 1 && $0.frame.height > 1 }
            .compactMap { window -> ScreenSource? in
                guard let pid = window.owningApplication?.processID,
                      let process = NativeIdentity.process(pid) else { return nil }
                let filter = SCContentFilter(desktopIndependentWindow: window)
                return ScreenSource(
                    id: "window:\(window.windowID)", instance: "\(process):window:\(window.windowID)", kind: "window",
                    title: window.title ?? window.owningApplication?.applicationName ?? "window",
                    width: Int(filter.contentRect.width * CGFloat(filter.pointPixelScale)),
                    height: Int(filter.contentRect.height * CGFloat(filter.pointPixelScale)),
                    scale: Double(filter.pointPixelScale),
                    processID: window.owningApplication?.processID
                )
            }
        return displays + windows
    }

    public func capture(sourceID: String, options: ScreenOptions = ScreenOptions(),
                        previousFingerprint: Data? = nil,
                        onMeasurement: ((CaptureTiming) -> Void)? = nil) async throws -> EncodedImage {
        guard Self.hasPermission else { throw CaptureError.permissionRequired }
        if content == nil { _ = try await refresh() }
        guard let content else { throw CaptureError.sourceMissing }
        let filter: SCContentFilter
        if sourceID.hasPrefix("display:"),
           let id = UInt32(sourceID.dropFirst(8)),
           let display = content.displays.first(where: { $0.displayID == id }) {
            filter = SCContentFilter(display: display, excludingWindows: [])
        } else if sourceID.hasPrefix("window:"),
                  let id = UInt32(sourceID.dropFirst(7)),
                  let window = content.windows.first(where: { $0.windowID == id }) {
            filter = SCContentFilter(desktopIndependentWindow: window)
        } else {
            throw CaptureError.sourceMissing
        }
        let bounds = CGRect(origin: .zero, size: filter.contentRect.size)
        let scale = CGFloat(filter.pointPixelScale)
        let selected = options.cropPixels.map {
            CGRect(x: $0.minX / scale, y: $0.minY / scale,
                   width: $0.width / scale, height: $0.height / scale)
        } ?? bounds
        try CaptureGeometry.validateCrop(selected, within: bounds)
        let size = try CaptureGeometry.fit(
            width: Int(selected.width * CGFloat(filter.pointPixelScale)),
            height: Int(selected.height * CGFloat(filter.pointPixelScale)),
            maxWidth: options.maxWidth
        )
        let configuration = SCStreamConfiguration()
        configuration.width = size.width
        configuration.height = size.height
        configuration.sourceRect = selected
        configuration.showsCursor = options.showsCursor
        configuration.scalesToFit = true
        configuration.ignoreShadowsSingleWindow = true
        let started = DispatchTime.now().uptimeNanoseconds
        let image = try await SCScreenshotManager.captureImage(
            contentFilter: filter, configuration: configuration
        )
        let captured = DispatchTime.now().uptimeNanoseconds
        let encoded = try ImageEncoder.encode(image, format: options.format, quality: options.quality,
                                             ifDifferentFrom: previousFingerprint)
        onMeasurement?(CaptureTiming(captureNS: captured - started,
                                     encodeNS: DispatchTime.now().uptimeNanoseconds - captured))
        return encoded
    }

    private func refresh() async throws -> SCShareableContent {
        guard Self.hasPermission else { throw CaptureError.permissionRequired }
        let refreshed = try await SCShareableContent.excludingDesktopWindows(false,
                                                                           onScreenWindowsOnly: true)
        content = refreshed
        return refreshed
    }
}
