// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "AsyncRay",
    platforms: [
        .macOS(.v14),
        .iOS(.v16),
        .tvOS(.v16),
        .watchOS(.v9),
        .visionOS(.v1)
    ],
    products: [
        .library(name: "AsyncRay", targets: ["AsyncRay"]),
    ],
    targets: [
        .target(
            name: "AsyncRay",
            path: "Sources"
        ),
        .testTarget(
            name: "AsyncRayTests",
            dependencies: ["AsyncRay"],
            path: "Tests"
        ),
    ],
    swiftLanguageVersions: [.v5, .version("6")]
)
