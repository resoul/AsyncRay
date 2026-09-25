load("@build_bazel_rules_swift//swift:swift.bzl", "swift_library", "swift_test")

swift_library(
    name = "AsyncRay",
    srcs = glob(["Sources/**/*.swift"]),
    module_name = "AsyncRay",
    copts = [
        "-strict-concurrency=complete",
    ],
    visibility = ["//visibility:public"],
)

swift_test(
    name = "AsyncRayTests",
    srcs = glob(["Tests/**/*.swift"]),
    deps = [":AsyncRay"],
)
