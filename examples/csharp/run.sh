#!/usr/bin/env bash
# Builds and runs the example in the official .NET 10 SDK container, for machines without a local
# .NET 10 SDK. Takes the same TL_* environment variables as `dotnet run` (see README.md).
#
#   TL_DEVELOPER_API_KEY=... TL_EMAIL=... TL_PASSWORD=... TL_SERVER=... ./run.sh
#
# - localhost / 127.0.0.1 in TL_BACKEND_URL and TL_STREAMS_URL become host.docker.internal, so the
#   example reaches a mock server running on this machine.
# - Secrets are passed by name (-e NAME), so their values never appear on a command line.
# - The source is mounted read-only; build output and the NuGet cache live in Docker volumes.
# - Ctrl+C / SIGINT / SIGTERM reach the .NET process (docker forwards them to tini, tini to dotnet),
#   so the example unsubscribes and exits 0.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
image="${DOTNET_SDK_IMAGE:-mcr.microsoft.com/dotnet/sdk:10.0}"

to_container() {
  local url=$1
  url=${url/:\/\/localhost/://host.docker.internal}
  url=${url/:\/\/127.0.0.1/://host.docker.internal}
  printf '%s' "$url"
}

env_args=(-e TL_DEVELOPER_API_KEY -e TL_EMAIL -e TL_PASSWORD -e TL_SERVER)
for name in TL_BACKEND_URL TL_STREAMS_URL; do
  if [[ -n "${!name:-}" ]]; then
    export "$name=$(to_container "${!name}")"
    env_args+=(-e "$name")
  fi
done

# Build output goes to stderr, so stdout carries only the example's own log lines.
exec docker run --rm --init --sig-proxy=true \
  --add-host=host.docker.internal:host-gateway \
  "${env_args[@]}" \
  -e DOTNET_NOLOGO=1 -e DOTNET_CLI_TELEMETRY_OPTOUT=1 \
  -v "$here":/src:ro \
  -v tl-streams-example-nuget:/root/.nuget/packages \
  -v tl-streams-example-build:/build \
  -w /src \
  "$image" \
  sh -c 'dotnet build StreamsExample.csproj -c Release --artifacts-path /build -nologo -v quiet 1>&2 \
    && exec dotnet /build/bin/StreamsExample/release/StreamsExample.dll'
