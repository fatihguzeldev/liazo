import AudioToolbox
import Foundation

public struct PCMFrame: Sendable {
    public let frames: UInt32
    public let bytes: Data
}

public enum AudioCaptureError: Error, LocalizedError {
    case unsupportedFormat
    case microphonePermission
    case hardware(String, OSStatus)

    public var errorDescription: String? {
        switch self {
        case .unsupportedFormat: "audio source returned an unsupported buffer format"
        case .microphonePermission: "allow microphone access for liazo in system settings"
        case let .hardware(operation, status): "\(operation) failed (\(status))"
        }
    }
}

public enum PCMEncoder {
    public static func encode(_ input: UnsafePointer<AudioBufferList>,
                              format: AudioStreamBasicDescription) throws -> PCMFrame {
        let channels = Int(format.mChannelsPerFrame)
        guard (1...8).contains(channels), format.mSampleRate.isFinite,
              (8000...384000).contains(format.mSampleRate),
              format.mFormatID == kAudioFormatLinearPCM,
              format.mFormatFlags & kAudioFormatFlagIsFloat != 0,
              format.mFormatFlags & kAudioFormatFlagIsBigEndian == 0,
              format.mBitsPerChannel == 32 else { throw AudioCaptureError.unsupportedFormat }
        let planar = format.mFormatFlags & kAudioFormatFlagIsNonInterleaved != 0
        guard format.mBytesPerFrame == (planar ? 4 : channels * 4) else {
            throw AudioCaptureError.unsupportedFormat
        }
        let buffers = UnsafeMutableAudioBufferListPointer(UnsafeMutablePointer(mutating: input))
        guard buffers.count == (planar ? channels : 1) else {
            throw AudioCaptureError.unsupportedFormat
        }
        var frames: Int?
        for buffer in buffers {
            let bufferChannels = planar ? 1 : channels
            guard buffer.mNumberChannels == bufferChannels,
                  buffer.mDataByteSize % UInt32(bufferChannels * 4) == 0 else {
                throw AudioCaptureError.unsupportedFormat
            }
            let count = Int(buffer.mDataByteSize) / (bufferChannels * 4)
            guard count <= 8192, count == 0 || buffer.mData != nil,
                  frames == nil || frames == count else { throw AudioCaptureError.unsupportedFormat }
            frames = count
        }
        let count = frames ?? 0
        var bytes = Data(count: count * channels * 4)
        if count > 0 {
            bytes.withUnsafeMutableBytes { output in
                if !planar || channels == 1 {
                    output.copyMemory(from: UnsafeRawBufferPointer(start: buffers[0].mData,
                                                                  count: output.count))
                } else {
                    let destination = output.bindMemory(to: UInt32.self)
                    for channel in 0..<channels {
                        let source = buffers[channel].mData!.assumingMemoryBound(to: UInt32.self)
                        for frame in 0..<count {
                            destination[frame * channels + channel] = source[frame]
                        }
                    }
                }
            }
        }
        return PCMFrame(frames: UInt32(count), bytes: bytes)
    }
}
