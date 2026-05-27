# Default recipe
default:
    @just --list

# Format code
fmt:
    cargo fmt

# Run linters (formatting check + clippy)
lint:
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings

# Fix lint issues automatically
lint-fix:
    cargo fmt
    cargo clippy --all-targets --fix --allow-dirty -- -D warnings

# Run unit tests
test:
    cargo test --lib

# Run all tests including e2e (requires env setup)
test-all:
    cargo nextest run

# Run e2e tests only
test-e2e:
    cargo nextest run --test '*'

# Build plugin .so
build:
    cargo build --lib

# Build plugin .so (release)
build-release:
    cargo build --release --lib

# Check compilation without building
check:
    cargo check --all-targets
