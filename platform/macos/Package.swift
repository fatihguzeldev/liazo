// swift-tools-version: 6.0
import PackageDescription
import Foundation

let info = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
    .appendingPathComponent("Info.plist").path

let package = Package(
    name: "LiazoCapture",
    platforms: [.macOS(.v14)],
    products: [
        .library(name: "LiazoCapture", type: .static, targets: ["LiazoCapture"]),
        .executable(name: "liazo-capture", targets: ["CaptureProbe"]),
        .executable(name: "capture-tests", targets: ["CaptureTests"])
    ],
    targets: [
        .target(name: "LiazoCapture"),
        .executableTarget(
            name: "CaptureProbe", dependencies: ["LiazoCapture"],
            linkerSettings: [.unsafeFlags(["-Xlinker", "-sectcreate", "-Xlinker", "__TEXT",
                                          "-Xlinker", "__info_plist", "-Xlinker", info])]
        ),
        .executableTarget(name: "CaptureTests", dependencies: ["LiazoCapture"],
                          path: "Tests/LiazoCaptureTests")
    ]
)
