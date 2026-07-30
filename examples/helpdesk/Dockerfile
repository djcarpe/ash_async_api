# Build context is the repository root, so the example can depend on ash_async_api by
# path and always run against the working tree. See `docker-compose.yml`.
FROM elixir:1.18-otp-27

# emqtt is fetched from git and built with rebar3.
RUN apt-get update \
  && apt-get install -y --no-install-recommends git build-essential \
  && rm -rf /var/lib/apt/lists/*

RUN mix local.hex --force && mix local.rebar --force

# QUIC support pulls in a large native build that plain MQTT over TCP does not need.
ENV BUILD_WITHOUT_QUIC=1
ENV MIX_ENV=dev

WORKDIR /app

# The library's mix.exs alone, with an empty lib/. That is enough for `mix deps.get` to
# resolve the path dependency and for `mix deps.compile` to build the hex dependencies —
# and it means editing the library source below does *not* invalidate the layer that
# compiles Ash, which is by far the slowest step.
COPY mix.exs mix.lock .formatter.exs README.md /app/lib_source/
RUN mkdir -p /app/lib_source/lib

WORKDIR /app/example
COPY examples/helpdesk/mix.exs /app/example/mix.exs

# Point the path dep at where the library actually landed in the image.
RUN sed -i 's|path: "../.."|path: "/app/lib_source"|' mix.exs

RUN mix deps.get
RUN mix deps.compile

# Now the library source. Only ash_async_api itself recompiles from here on.
COPY lib /app/lib_source/lib
RUN mix deps.compile ash_async_api --force

COPY examples/helpdesk/config /app/example/config
COPY examples/helpdesk/lib /app/example/lib
RUN mix compile

COPY examples/helpdesk/bin /app/example/bin
RUN chmod +x /app/example/bin/*

CMD ["bin/start"]
