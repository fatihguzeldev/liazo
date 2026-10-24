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
            case "audio-sources":
                try emit(AudioSources.list())
            case "audio":
                guard arguments.count >= 2 else { throw CaptureError.invalidRequest }
                let seconds = arguments.count > 2 ? Double(arguments[2]) : 2
                guard let seconds, seconds.isFinite, seconds > 0, seconds <= 10 else {
                    throw CaptureError.invalidRequest
                }
                let stats = AudioStats()
                let audio = AudioCapture()
                try audio.start(sourceID: arguments[1]) { stats.receive($0) }
                defer { audio.stop() }
                try await Task.sleep(for: .seconds(seconds))
                try emit(stats.snapshot())
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
                print("liazo-capture status | sources | audio-sources | request-permissions")
                print("liazo-capture capture <source> <output.png> [max-width] | audio <source> [seconds]")
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

final class AudioStats: @unchecked Sendable {
    private let lock = NSLock()
    private var chunks = 0
    private var frames: UInt64 = 0
    private var failures = 0

    func receive(_ result: Result<AudioChunk, Error>) {
        lock.lock()
        defer { lock.unlock() }
        switch result {
        case let .success(chunk):
            chunks += 1
            frames += UInt64(chunk.frames)
        case .failure:
            failures += 1
        }
    }

    func snapshot() -> [String: UInt64] {
        lock.lock()
        defer { lock.unlock() }
        return ["chunks": UInt64(chunks), "frames": frames, "errors": UInt64(failures)]
    }
}
