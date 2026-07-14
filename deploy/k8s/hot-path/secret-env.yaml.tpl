# Dev credentials for hot-path trackers (same data plane as cold-path local stack).
apiVersion: v1
kind: Secret
metadata:
  name: espx-edge-secrets
  namespace: espx-edge
  labels:
    app.kubernetes.io/part-of: espx
    app.kubernetes.io/component: hot-path
type: Opaque
stringData:
  DB_DSN: "postgres://ad_event_processor_user:secure_pass_123@${host_ip}:5430/ad_event_processor?sslmode=disable"
  REDIS_PASSWORD: "${redis_password}"
  TOKEN_SYMMETRIC_KEY: "01234567890123456789012345678901"
