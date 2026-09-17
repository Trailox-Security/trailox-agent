# trailox-agent
#
# Runtime image is the distroless, chiseled .NET runtime: no shell, no package manager, a
# non-root default user, and a CA bundle. The container runs read-only with every capability
# dropped (see deploy/docker-compose.yml); the only writable path is a tmpfs at /tmp, used for
# the liveness file.

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG VERSION=0.1.0
WORKDIR /src
COPY src/Trailox.Agent/Trailox.Agent.csproj src/Trailox.Agent/
RUN dotnet restore src/Trailox.Agent/Trailox.Agent.csproj
COPY src/ src/
RUN dotnet publish src/Trailox.Agent/Trailox.Agent.csproj -c Release -o /app -p:Version=$VERSION -p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0-noble-chiseled AS final
WORKDIR /app
COPY --from=build /app .
# Fixed, unprivileged, no passwd entry needed. Matches the compose/Helm examples.
USER 10001:10001
ENV TRAILOX_CONFIG=/etc/trailox/agent.yaml \
    TRAILOX_HEALTH_FILE=/tmp/trailox-agent.health \
    DOTNET_EnableDiagnostics=0
# exec form: there is no shell in this image. The subcommand reads the liveness file.
HEALTHCHECK --interval=60s --timeout=5s --start-period=30s --retries=3 \
    CMD ["dotnet", "trailox-agent.dll", "healthcheck"]
ENTRYPOINT ["dotnet", "trailox-agent.dll"]
CMD ["run"]
