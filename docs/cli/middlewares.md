# Taegis Magic

## Middlewares

### Logging Middleware

Taegis Magic supports logging AIOHTTP Request and Response headers to INFO logs in the `taegis_magic` logger.

```
taegis configure middlewares toggle logging --on
```

```
taegis configure middlewares toggle logging --off
```

Logs may be sent to standard out with the `--verbose`,`--debug`, and `--trace` logging flags passed to the `taegis` command.

```
taegis --verbose subjects current-subject
```

### Retry Middleware

Taegis Magic provides a retry on exponential backoff for REQUEST_TIMEOUT (408), INTERNAL_SERVER_ERROR (500), BAD_GATEWAY (502), SERVICE_UNAVAILABLE (503), GATEWAY_TIMEOUT (504).  If TOO_MANY_REQUESTS (429) is encounted, the value in the "Retry-After" header will be used as the backoff timer.

The SDK may be configured to provide a maximum amount of seconds or calls before giving up.  Defaults to 10 seconds with no call maximum.

```
taegis configure middlewares toggle retry --on
```

```
taegis configure middlewares toggle retry --off
```

Change the maximum amount of total seconds for retry.

```
taegis configure middlewares retry max_time 30
```

Change the maximum amount of calls for retry.

```
taegis configure middlewares retry max_tries 3
```
