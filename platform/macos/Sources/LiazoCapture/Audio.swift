import AudioToolbox
@preconcurrency import AVFoundation
@preconcurrency import CoreAudio
import Foundation

public struct AudioChunk: Sendable {
    public let sourceID: String
    public let sequence: UInt64
    public let samplePosition: UInt64
    public let hostTimeNs: UInt64
    public let sampleRate: Double
    public let channels: UInt32
    public let frames: UInt32
    public let discontinuity: Bool
    public let bytes: Data
}

// Called on the audio queue. The sink must return promptly and must not stop capture synchronously.
public typealias AudioSink = @Sendable (Result<AudioChunk, Error>) -> Void

private final class AudioDelivery: @unchecked Sendable {
    let sourceID: String
    let sink: AudioSink
    private let lock = NSLock()
    private var sequence: UInt64 = 0
    private var nextPosition: UInt64 = 0
    private var active = true

    init(sourceID: String, sink: @escaping AudioSink) {
        self.sourceID = sourceID
        self.sink = sink
    }

    func stop() { lock.lock(); active = false; lock.unlock() }

    func receive(_ buffers: UnsafePointer<AudioBufferList>, format: AudioStreamBasicDescription,
                 timestamp: AudioTimeStamp) {
        do {
            let pcm = try PCMEncoder.encode(buffers, format: format)
            guard pcm.frames > 0 else { return }
            lock.lock()
            guard active else { lock.unlock(); return }
            let hasTime = timestamp.mFlags.contains(.sampleTimeValid) &&
                          timestamp.mSampleTime.isFinite && timestamp.mSampleTime >= 0 &&
                          timestamp.mSampleTime < Double(UInt64.max)
            let position = hasTime ? UInt64(timestamp.mSampleTime) : nextPosition
            let discontinuity = sequence > 0 && position != nextPosition
            nextPosition = position + UInt64(pcm.frames)
            sequence += 1
            let currentSequence = sequence
            let host = timestamp.mFlags.contains(.hostTimeValid)
                ? AudioConvertHostTimeToNanos(timestamp.mHostTime)
                : DispatchTime.now().uptimeNanoseconds
            sink(.success(AudioChunk(
                sourceID: sourceID, sequence: currentSequence, samplePosition: position,
                hostTimeNs: host, sampleRate: format.mSampleRate, channels: format.mChannelsPerFrame,
                frames: pcm.frames, discontinuity: discontinuity, bytes: pcm.bytes
            )))
            lock.unlock()
        } catch {
            lock.lock()
            if active { sink(.failure(error)) }
            lock.unlock()
        }
    }
}

@MainActor public final class AudioCapture {
    private var tapID: AudioObjectID = kAudioObjectUnknown
    private var aggregateID: AudioObjectID = kAudioObjectUnknown
    private var ioProc: AudioDeviceIOProcID?
    private var engine: AVAudioEngine?
    private var microphoneTapInstalled = false
    private var delivery: AudioDelivery?
    private let queue = DispatchQueue(label: "app.liazo.audio", qos: .userInitiated)

    public init() {}

    public func start(sourceID: String, sink: @escaping AudioSink) throws {
        stop()
        let delivery = AudioDelivery(sourceID: sourceID, sink: sink)
        self.delivery = delivery
        do {
            if sourceID.hasPrefix("audio:input:"),
               let device = UInt32(sourceID.dropFirst(12)) {
                try startMicrophone(device: device, delivery: delivery)
            } else {
                try startTap(sourceID: sourceID, delivery: delivery)
            }
        } catch {
            stop()
            throw error
        }
    }

    public func stop() {
        delivery?.stop()
        if let engine {
            if microphoneTapInstalled { engine.inputNode.removeTap(onBus: 0) }
            engine.stop()
        }
        microphoneTapInstalled = false
        engine = nil
        if aggregateID != kAudioObjectUnknown {
            if let ioProc {
                AudioDeviceStop(aggregateID, ioProc)
                AudioDeviceDestroyIOProcID(aggregateID, ioProc)
            }
            AudioHardwareDestroyAggregateDevice(aggregateID)
        }
        if tapID != kAudioObjectUnknown { AudioHardwareDestroyProcessTap(tapID) }
        ioProc = nil
        aggregateID = kAudioObjectUnknown
        tapID = kAudioObjectUnknown
        delivery = nil
    }

    private func startTap(sourceID: String, delivery: AudioDelivery) throws {
        let description: CATapDescription
        if sourceID == "audio:system" {
            let own = (try? HAL.ids(AudioObjectID(kAudioObjectSystemObject),
                                    kAudioHardwarePropertyProcessObjectList))?.filter {
                (try? HAL.value($0, kAudioProcessPropertyPID, initial: Int32(0))) == getpid()
            } ?? []
            description = CATapDescription(stereoGlobalTapButExcludeProcesses: own)
        } else if sourceID.hasPrefix("audio:process:"),
                  let object = UInt32(sourceID.dropFirst(14)) {
            description = CATapDescription(stereoMixdownOfProcesses: [object])
        } else { throw CaptureError.sourceMissing }
        description.name = "liazo audio"
        description.isPrivate = true
        description.muteBehavior = .unmuted
        try HAL.check(AudioHardwareCreateProcessTap(description, &tapID), "start system audio")
        let format = try HAL.value(tapID, kAudioTapPropertyFormat,
                                   initial: AudioStreamBasicDescription())
        let uid = try HAL.string(tapID, kAudioTapPropertyUID)
        let definition: [String: Any] = [
            kAudioAggregateDeviceNameKey: "liazo audio",
            kAudioAggregateDeviceUIDKey: "app.liazo.\(UUID().uuidString)",
            kAudioAggregateDeviceIsPrivateKey: true,
            kAudioAggregateDeviceTapAutoStartKey: true,
            kAudioAggregateDeviceTapListKey: [[kAudioSubTapUIDKey: uid,
                                              kAudioSubTapDriftCompensationKey: true]]
        ]
        try HAL.check(AudioHardwareCreateAggregateDevice(definition as CFDictionary, &aggregateID),
                      "create audio input")
        try HAL.check(AudioDeviceCreateIOProcIDWithBlock(&ioProc, aggregateID, queue) {
            @Sendable _, input, time, _, _ in
            delivery.receive(input, format: format, timestamp: time.pointee)
        }, "attach audio input")
        try HAL.check(AudioDeviceStart(aggregateID, ioProc), "start audio input")
    }

    private func startMicrophone(device: AudioObjectID, delivery: AudioDelivery) throws {
        guard AVCaptureDevice.authorizationStatus(for: .audio) == .authorized else {
            throw AudioCaptureError.microphonePermission
        }
        let engine = AVAudioEngine()
        self.engine = engine
        let input = engine.inputNode
        guard let unit = input.audioUnit else { throw CaptureError.sourceMissing }
        var device = device
        try HAL.check(AudioUnitSetProperty(unit, kAudioOutputUnitProperty_CurrentDevice,
                                          kAudioUnitScope_Global, 0, &device,
                                          UInt32(MemoryLayout<AudioObjectID>.size)),
                      "select microphone")
        let format = input.outputFormat(forBus: 0)
        input.installTap(onBus: 0, bufferSize: 1024, format: format) { @Sendable buffer, time in
            delivery.receive(buffer.audioBufferList, format: buffer.format.streamDescription.pointee,
                             timestamp: time.audioTimeStamp)
        }
        microphoneTapInstalled = true
        engine.prepare()
        try engine.start()
    }
}
