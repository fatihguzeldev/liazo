import Foundation
import AVFoundation
import CoreGraphics
import LiazoCapture

@main struct CaptureProbe {
    @MainActor static func main() async {
        do {
            let arguments = Array(CommandLine.arguments.dropFirst())
            switch arguments.first {
            case "status":
                try emit([
                    "screen": ScreenCapture.hasPermission ? "allowed" : "not_allowed",
                    "microphone": String(describing: AVCaptureDevice.authorizationStatus(for: .audio))
                ])
            case "request-permissions":
                let screen = CGRequestScreenCaptureAccess()
                let microphone = await AVCaptureDevice.requestAccess(for: .audio)
                try emit(["screen": screen, "microphone": microphone])
            case "sources":
                try emit(await ScreenCapture().sources())
            case "capture":
                guard arguments.count >= 3 else { throw CaptureError.invalidRequest }
                var options = ScreenOptions()
                if arguments.count > 3 {
                    guard let width = Int(arguments[3]) else { throw CaptureError.invalidRequest }
                    options.maxWidth = width
                }
                let started = DispatchTime.now().uptimeNanoseconds
                let image = try await ScreenCapture().capture(sourceID: arguments[1], options: options)
                guard let bytes = image.bytes else { throw CaptureError.encodingFailed }
                try bytes.write(to: URL(fileURLWithPath: arguments[2]), options: .atomic)
                try emit([
                    "width": image.width, "height": image.height, "bytes": bytes.count,
                    "capture_ms": Int((DispatchTime.now().uptimeNanoseconds - started) / 1_000_000)
                ])
            default:
                print("liazo-capture status | sources | request-permissions | capture <source> <output.png> [max-width]")
            }
        } catch {
            FileHandle.standardError.write(Data((error.localizedDescription.lowercased() + "\n").utf8))
            exit(1)
        }
    }

    static func emit<T: Encodable>(_ value: T) throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let encoded = try encoder.encode(value)
        FileHandle.standardOutput.write(encoded + Data([10]))
    }
}
