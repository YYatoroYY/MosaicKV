FROM ubuntu:20.04 AS dependencies
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ make libboost-dev libzmq3-dev libisal-dev libmemcached-dev memcached ca-certificates \
    && rm -rf /var/lib/apt/lists/*

FROM dependencies AS build
WORKDIR /build
COPY Makefile ./
COPY src ./src
COPY tests/*.cpp ./tests/
COPY scripts/ec_node_service /usr/local/bin/ec_node_service
RUN chmod +x /usr/local/bin/ec_node_service
RUN make -j2 test

FROM ubuntu:20.04 AS runtime
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends libzmq5 libisal2 libmemcached11 memcached \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/src/ /build/src/
COPY --from=build /build/bin/ /usr/local/bin/
COPY --from=build /usr/local/bin/ec_node_service /usr/local/bin/ec_node_service
WORKDIR /app
CMD ["ec_proxy"]
