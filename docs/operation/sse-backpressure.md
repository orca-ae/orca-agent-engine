# SSE backpressure policy (registry-server)

> How the registry's SSE bridge behaves when a client cannot keep up.

## Policy: bounded buffer + drop-and-resync

Each `GET /v1/sessions/:id/events/stream` connection has a per-connection event buffer
limited to **256 events**. The buffer drains as the HTTP client reads. If the
buffer fills AND the oldest unsent event in it is older than **5 seconds**, the
handler immediately aborts its transcript tail and closes the connection.
`orca-beta` streams first emit one extension `event: drop` frame:

```
event: drop
data: {"reason":"client-too-slow","last_seq":"<offset>"}

```

The `last_seq` value is the last complete event frame accepted by the HTTP
writer, not the newest event still waiting in the buffer. The client reconnects
with `Last-Event-ID: <offset>` (per the SSE spec). The handler resumes via the
selected transcript backend's `store.tail({ fromCursor: "<offset>" })`. The
resume is inclusive: the frame whose `id` is `<offset>` is sent again, so the
client receives that boundary frame twice. The transcript backend's durability
guarantees no event is lost — the client sees a brief pause.

Default Claude-compatible streams do not emit the non-standard `drop` event;
they close immediately and rely on the SSE client's own last-event ID for
resumption.

## Heartbeats

Every 15 seconds an idle stream emits `:heartbeat\n\n` (an SSE comment) so
intermediaries don't time out idle TCP connections.

## Tunables (env)

| Var                | Default | Meaning                                                   |
| ------------------ | ------- | --------------------------------------------------------- |
| `SSE_BUFFER_SIZE`  | 256     | Max events queued per connection before we consider drop. |
| `SSE_DROP_AGE_MS`  | 5000    | Oldest-buffered-event age threshold before we drop.       |
| `SSE_HEARTBEAT_MS` | 15000   | Heartbeat interval.                                       |

## Metrics

| Metric                                    | Type      | Labels   | Meaning                              |
| ----------------------------------------- | --------- | -------- | ------------------------------------ |
| `registry_service_sse_connections_active` | Gauge     | —        | Currently open SSE connections       |
| `registry_service_sse_buffer_depth`       | Histogram | —        | Buffer occupancy at flush time       |
| `registry_service_sse_drop_total`         | Counter   | `reason` | Connection drops (`client-too-slow`) |
