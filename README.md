# Fluentd Exporter for Prometheus

This is a simple server that scrapes Fluentd worker stats and exports them via HTTP for
Prometheus consumption.

## Getting Started

To run it:

```bash
./fluentd_exporter [flags]
```

Help on flags:

```bash
./fluentd_exporter --help
```

## Usage

### HTTP stats URL

Specify custom URLs for the Fluentd stats port using the `--fluentd.scrape-uri`
flag.

```bash
fluentd_exporter --fluentd.scrape-uri="http://localhost:24231/metrics"
```

### Building

```bash
make build
```

### Testing

```bash
make test
```

## License

Apache License 2.0, see [LICENSE](https://github.com/prometheus/fluentd_exporter/blob/master/LICENSE).