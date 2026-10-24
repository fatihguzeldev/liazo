import AppKit
import CoreAudio
import Foundation

enum HAL {
    static func check(_ status: OSStatus, _ operation: String) throws {
        if status != noErr { throw AudioCaptureError.hardware(operation, status) }
    }

    static func value<T>(_ id: AudioObjectID, _ selector: AudioObjectPropertySelector,
                         scope: AudioObjectPropertyScope = kAudioObjectPropertyScopeGlobal,
                         initial: T) throws -> T {
        var address = AudioObjectPropertyAddress(mSelector: selector, mScope: scope,
                                                mElement: kAudioObjectPropertyElementMain)
        var value = initial
        var size = UInt32(MemoryLayout<T>.size)
        try withUnsafeMutableBytes(of: &value) { bytes in
            try check(AudioObjectGetPropertyData(id, &address, 0, nil, &size, bytes.baseAddress!),
                      "read audio property")
        }
        guard size == MemoryLayout<T>.size else { throw AudioCaptureError.unsupportedFormat }
        return value
    }

    static func ids(_ id: AudioObjectID, _ selector: AudioObjectPropertySelector) throws -> [AudioObjectID] {
        var address = AudioObjectPropertyAddress(mSelector: selector, mScope: kAudioObjectPropertyScopeGlobal,
                                                mElement: kAudioObjectPropertyElementMain)
        var size: UInt32 = 0
        try check(AudioObjectGetPropertyDataSize(id, &address, 0, nil, &size), "list audio sources")
        guard size <= 16384, size % 4 == 0 else { throw AudioCaptureError.unsupportedFormat }
        var result = [AudioObjectID](repeating: 0, count: Int(size) / 4)
        if size > 0 {
            try result.withUnsafeMutableBytes {
                try check(AudioObjectGetPropertyData(id, &address, 0, nil, &size, $0.baseAddress!),
                          "list audio sources")
            }
        }
        return Array(result.prefix(Int(size) / 4))
    }

    static func string(_ id: AudioObjectID, _ selector: AudioObjectPropertySelector) throws -> String {
        let value: Unmanaged<CFString>? = try value(id, selector, initial: Optional<Unmanaged<CFString>>.none)
        guard let value else { throw AudioCaptureError.unsupportedFormat }
        return value.takeRetainedValue() as String
    }

    static func hasInput(_ device: AudioObjectID) -> Bool {
        var address = AudioObjectPropertyAddress(mSelector: kAudioDevicePropertyStreams,
                                                mScope: kAudioDevicePropertyScopeInput,
                                                mElement: kAudioObjectPropertyElementMain)
        var size: UInt32 = 0
        return AudioObjectGetPropertyDataSize(device, &address, 0, nil, &size) == noErr && size > 0
    }
}

public struct AudioSource: Codable, Sendable {
    public let id: String
    public let kind: String
    public let title: String
    public let uid: String?
    public let processID: Int32?
}

@MainActor public enum AudioSources {
    public static func list() throws -> [AudioSource] {
        let system = AudioObjectID(kAudioObjectSystemObject)
        var sources = [AudioSource(id: "audio:system", kind: "system_audio",
                                   title: "system audio", uid: nil, processID: nil)]
        for object in try HAL.ids(system, kAudioHardwarePropertyProcessObjectList) {
            guard let pid = try? HAL.value(object, kAudioProcessPropertyPID, initial: Int32(0)),
                  pid != getpid(),
                  let running = try? HAL.value(object, kAudioProcessPropertyIsRunningOutput, initial: UInt32(0)),
                  running != 0 else { continue }
            let application = NSRunningApplication(processIdentifier: pid)
            sources.append(AudioSource(
                id: "audio:process:\(object)", kind: "process_audio",
                title: application?.localizedName ?? "process \(pid)",
                uid: try? HAL.string(object, kAudioProcessPropertyBundleID), processID: pid
            ))
        }
        for device in try HAL.ids(system, kAudioHardwarePropertyDevices) where HAL.hasInput(device) {
            sources.append(AudioSource(
                id: "audio:input:\(device)", kind: "microphone",
                title: (try? HAL.string(device, kAudioObjectPropertyName)) ?? "microphone",
                uid: try? HAL.string(device, kAudioDevicePropertyDeviceUID), processID: nil
            ))
        }
        return sources
    }
}
