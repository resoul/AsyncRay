.PHONY: help test test-mac test-phone test-tv coverage

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "%-25s %s\n", $$1, $$2}'

test: ## Swift Package tests
	swift test

test-mac: ## Tests on macOS
	xcodebuild test -scheme AsyncRay -destination 'platform=macOS'

test-phone: ## Tests on iPhone simulator
	xcodebuild test -scheme AsyncRay -destination 'platform=iOS Simulator,name=iPhone 18 Pro'

test-tv: ## Tests on Apple TV simulator
	xcodebuild test -scheme AsyncRay -destination 'platform=tvOS Simulator,name=Apple TV 4K (3rd generation)'

coverage: ## Run tests and display code coverage report
	swift test --enable-code-coverage
	@BIN=$$(find .build -name "AsyncRayPackageTests" -type f -perm +111 | grep -v "\.dSYM" | head -n 1); \
	PROF=$$(find .build -name "default.profdata" | head -n 1); \
	xcrun llvm-cov report "$$BIN" --instr-profile="$$PROF" --ignore-filename-regex='\.build|Tests'

.PHONY: build-swift6 test-swift6 test-swift5
build-swift6: ## Build library in Swift 6 mode with complete checking and warnings as errors
	swift build --target AsyncRay -Xswiftc -swift-version -Xswiftc 6 -Xswiftc -strict-concurrency=complete -Xswiftc -warnings-as-errors

test-swift6: ## Run all tests in Swift 6 language mode
	swift test -Xswiftc -swift-version -Xswiftc 6 -Xswiftc -strict-concurrency=complete

test-swift5: ## Verify Swift 5 language compatibility using the installed compiler
	swift test -Xswiftc -swift-version -Xswiftc 5 -Xswiftc -strict-concurrency=complete

SWIFT_FORMAT ?= xcrun swift-format
SWIFT_PATHS ?= Sources Tests

.PHONY: lint-format format-swift
lint-format: ## Enforce Swift formatting and policy rules
	$(SWIFT_FORMAT) lint --strict --configuration .swift-format --recursive $(SWIFT_PATHS)

format-swift: ## Format selected Swift paths; override with SWIFT_PATHS=path/to/File.swift
	$(SWIFT_FORMAT) format --in-place --configuration .swift-format --recursive $(SWIFT_PATHS)
