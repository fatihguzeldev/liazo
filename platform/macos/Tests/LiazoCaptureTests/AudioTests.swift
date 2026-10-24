import AVFoundation
import Foundation
import LiazoCapture

func checkAudio() throws {
    let format = AVAudioFormat(standardFormatWithSampleRate: 48000, channels: 2)!
    let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: 2)!
    buffer.frameLength = 2
    buffer.floatChannelData![0][0] = 0.25
    buffer.floatChannelData![0][1] = -0.25
    buffer.floatChannelData![1][0] = 0.5
    buffer.floatChannelData![1][1] = -0.5
    let encoded = try PCMEncoder.encode(buffer.audioBufferList,
                                       format: format.streamDescription.pointee)
    let values = encoded.bytes.withUnsafeBytes { bytes in
        stride(from: 0, to: bytes.count, by: 4).map {
            Float(bitPattern: UInt32(littleEndian: bytes.loadUnaligned(fromByteOffset: $0, as: UInt32.self)))
        }
    }
    try expect(encoded.frames == 2 && values == [0.25, 0.5, -0.25, -0.5],
               "planar input was not interleaved in sample order")
    var unsupported = format.streamDescription.pointee
    unsupported.mChannelsPerFrame = 100
    try rejects({ _ = try PCMEncoder.encode(buffer.audioBufferList, format: unsupported) },
                "unbounded channel count accepted")
    unsupported = format.streamDescription.pointee
    unsupported.mBytesPerFrame += 4
    try rejects({ _ = try PCMEncoder.encode(buffer.audioBufferList, format: unsupported) },
                "padded samples were interpreted as packed float32")
    unsupported = format.streamDescription.pointee
    unsupported.mSampleRate = .nan
    try rejects({ _ = try PCMEncoder.encode(buffer.audioBufferList, format: unsupported) },
                "invalid sample rate accepted")
    print("audio: planar samples, channel bounds, and rate validation passed")
}
