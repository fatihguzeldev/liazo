import AppKit
import AVFoundation
import CLiazoCapture
import Foundation

@_cdecl("liazo_native_clock_ns")
public func nativeClockNS() -> UInt64 { DispatchTime.now().uptimeNanoseconds }

private final class Reply: @unchecked Sendable {
    let id: UInt64
    let callback: LiazoCallback
    let user: UnsafeMutableRawPointer?

    init(_ id: UInt64, _ callback: @escaping LiazoCallback, _ user: UnsafeMutableRawPointer?) {
        self.id = id; self.callback = callback; self.user = user
    }

    func send(_ metadata: Data, payload: Data = Data(), status: Int32 = 0) {
        metadata.withUnsafeBytes { meta in
            payload.withUnsafeBytes { bytes in
                callback(user, id, status, meta.bindMemory(to: UInt8.self).baseAddress, meta.count,
                         bytes.bindMemory(to: UInt8.self).baseAddress, bytes.count)
            }
        }
    }

    func json(_ value: [String: Any], payload: Data = Data(), status: Int32 = 0) {
        do {
            send(try JSONSerialization.data(withJSONObject: value), payload: payload, status: status)
        } catch {
            send(Data("{\"error\":\"native metadata encoding failed\"}".utf8), status: 1)
        }
    }

    func fail(_ error: Error) { json(["error": error.localizedDescription], status: 1) }
}

@MainActor private final class BridgeState {
    static let shared = BridgeState()
    let screen = ScreenCapture()
    var imagesInFlight = Set<String>()
    var sourcesInFlight = false
    var audio: [UInt64: AudioCapture] = [:]
}

@_cdecl("liazo_native_sources")
public func nativeSources(_ id: UInt64, _ callback: LiazoCallback?, _ user: UnsafeMutableRawPointer?) {
    guard let callback else { return }
    let reply = Reply(id, callback, user)
    DispatchQueue.main.async {
        Task { @MainActor in
            let state = BridgeState.shared
            guard !state.sourcesInFlight else {
                reply.json(["error": "source discovery is busy"], status: 1); return
            }
            state.sourcesInFlight = true
            defer { state.sourcesInFlight = false }
            do {
                var screenError: String?
                var screens: [ScreenSource] = []
                do { screens = try await BridgeState.shared.screen.sources() }
                catch { screenError = error.localizedDescription }
                let audio = try AudioSources.list()
                struct Catalog: Encodable {
                    let screens: [ScreenSource]
                    let audio: [AudioSource]
                    let screen_error: String?
                    let screen_allowed: Bool
                    let microphone_allowed: Bool
                }
                let catalog = Catalog(screens: screens, audio: audio, screen_error: screenError,
                                      screen_allowed: ScreenCapture.hasPermission,
                                      microphone_allowed: AVCaptureDevice.authorizationStatus(for: .audio) == .authorized)
                reply.send(try JSONEncoder().encode(catalog))
            } catch { reply.fail(error) }
        }
    }
}

@_cdecl("liazo_native_image")
public func nativeImage(_ id: UInt64, _ source: UnsafePointer<CChar>?,
                        _ options: UnsafePointer<LiazoImageOptions>?,
                        _ callback: LiazoCallback?, _ user: UnsafeMutableRawPointer?) {
    guard let callback else { return }
    let reply = Reply(id, callback, user)
    guard let source, let options else { reply.fail(CaptureError.invalidRequest); return }
    let sourceID = String(cString: source)
    let copied = options.pointee
    let previous: Data? = copied.has_previous == 0 ? nil : withUnsafeBytes(of: copied.previous) { Data($0) }
    DispatchQueue.main.async {
        Task { @MainActor in
            let state = BridgeState.shared
            guard state.imagesInFlight.count < 2, !state.imagesInFlight.contains(sourceID) else {
                reply.json(["error": "capture is busy"], status: 1); return
            }
            state.imagesInFlight.insert(sourceID)
            defer { state.imagesInFlight.remove(sourceID) }
            do {
                guard copied.format <= 1, copied.quality <= 100 else { throw CaptureError.invalidRequest }
                var settings = ScreenOptions()
                settings.maxWidth = copied.max_width == 0 ? nil : Int(copied.max_width)
                settings.format = copied.format == 0 ? .png : .jpeg
                settings.quality = Double(copied.quality) / 100
                settings.showsCursor = copied.include_cursor != 0
                if copied.has_crop != 0 {
                    settings.cropPixels = CGRect(x: Int(copied.crop_x), y: Int(copied.crop_y),
                                                 width: Int(copied.crop_width), height: Int(copied.crop_height))
                }
                let started = DispatchTime.now().uptimeNanoseconds
                let image = try await state.screen.capture(sourceID: sourceID, options: settings,
                                                          previousFingerprint: previous)
                reply.json([
                    "width": image.width, "height": image.height, "format": image.format.rawValue,
                    "fingerprint": image.fingerprint.map { String(format: "%02x", $0) }.joined(),
                    "requested_ns": started, "completed_ns": DispatchTime.now().uptimeNanoseconds,
                    "unchanged": image.bytes == nil
                ], payload: image.bytes ?? Data())
            } catch { reply.fail(error) }
        }
    }
}

@_cdecl("liazo_native_audio_start")
public func nativeAudioStart(_ id: UInt64, _ source: UnsafePointer<CChar>?,
                             _ callback: LiazoCallback?, _ user: UnsafeMutableRawPointer?) {
    guard let callback else { return }
    let reply = Reply(id, callback, user)
    guard let source else { reply.fail(CaptureError.invalidRequest); return }
    let sourceID = String(cString: source)
    DispatchQueue.main.async {
        let state = BridgeState.shared
        guard state.audio.count < 8, state.audio[id] == nil else {
            reply.json(["error": "audio capture limit reached"], status: 1); return
        }
        do {
            let capture = AudioCapture()
            try capture.start(sourceID: sourceID) { result in
                switch result {
                case let .failure(error): reply.fail(error)
                case let .success(chunk):
                    reply.json([
                        "sequence": chunk.sequence, "sample_position": chunk.samplePosition,
                        "host_time_ns": chunk.hostTimeNs, "sample_rate": chunk.sampleRate,
                        "channels": chunk.channels, "frames": chunk.frames,
                        "discontinuity": chunk.discontinuity
                    ], payload: chunk.bytes)
                }
            }
            state.audio[id] = capture
            reply.json(["ready": true], status: 2)
        } catch { reply.fail(error) }
    }
}

@_cdecl("liazo_native_audio_stop")
public func nativeAudioStop(_ id: UInt64, _ callback: LiazoCallback?, _ user: UnsafeMutableRawPointer?) {
    guard let callback else { return }
    let reply = Reply(id, callback, user)
    DispatchQueue.main.async {
        BridgeState.shared.audio.removeValue(forKey: id)?.stop()
        reply.json(["stopped": true])
    }
}

@_cdecl("liazo_native_runloop")
public func nativeRunloop() {
    precondition(Thread.isMainThread)
    MainActor.assumeIsolated {
        NSApplication.shared.setActivationPolicy(.prohibited)
        _ = BridgeState.shared
        CFRunLoopRun()
    }
}

@_cdecl("liazo_native_stoploop")
public func nativeStoploop() {
    DispatchQueue.main.async {
        for capture in BridgeState.shared.audio.values { capture.stop() }
        BridgeState.shared.audio.removeAll()
        CFRunLoopStop(CFRunLoopGetMain())
    }
}
