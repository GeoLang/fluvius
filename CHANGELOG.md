# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased] - 2026-08-02

### Added

- `[pipeline.window]` and `[pipeline.watermark]` drive the runner: tumbling,
  sliding, session or count windows expire stateful operators, and events
  older than `max_lateness_secs` are dropped.
- `rate_limit` topology operator, a token bucket over the whole stream that drops
  what it rejects. Backed by `fluvius_core::operator::RateLimitOperator`.
- `[pipeline.replay]` now runs: it replaces the source with a recorded JSON lines
  file paced by the event timestamps, `speed = inf` for max speed.

### Changed

- 2026-08-15: docs badge is 187 tests.
- Trajectory emits a `stop` alert when an entity stays below 1 m/s for 30 s,
  and includes a 3-point smoothed position on each update.
- `map_match` is a topology operator: snap each event onto configured road
  segments and forward the snapped coordinates.

- `[pipeline.metrics]` and `[pipeline.checkpoint]` fail the run with an explanation
  instead of being silently ignored. Neither has a runner to attach to.
- `PipelineConfig::metrics` is now `Option<MetricsConfig>`, so an absent section no
  longer reads as an enabled metrics endpoint.

## [0.1.0] - 2026-05-30

### Added

- Initial release.
