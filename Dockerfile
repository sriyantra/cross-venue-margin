FROM rust:1.100-trixie AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
COPY migrations ./migrations
RUN cargo build --release --locked --bin cross-venue-margin

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=build /app/target/release/cross-venue-margin /cross-venue-margin
ENV BIND=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["/cross-venue-margin"]
