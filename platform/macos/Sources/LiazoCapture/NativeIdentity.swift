import CoreGraphics
import Darwin
import Foundation

enum NativeIdentity {
    static func process(_ pid: pid_t) -> String? {
        var info = proc_bsdinfo()
        let size = Int32(MemoryLayout<proc_bsdinfo>.size)
        let copied = withUnsafeMutablePointer(to: &info) {
            proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, $0, size)
        }
        guard copied == size else { return nil }
        return "process:\(pid):\(info.pbi_start_tvsec):\(info.pbi_start_tvusec)"
    }

    static func display(_ id: CGDirectDisplayID) -> String {
        "display:\(id):\(CGDisplayVendorNumber(id)):\(CGDisplayModelNumber(id)):\(CGDisplaySerialNumber(id)):\(CGDisplayUnitNumber(id))"
    }
}
