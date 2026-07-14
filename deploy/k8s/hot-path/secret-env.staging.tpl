# Staging credentials for hot-path trackers (external managed data plane).
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
  DB_DSN: "${db_dsn}"
  REDIS_PASSWORD: "${redis_password}"
  TOKEN_SYMMETRIC_KEY: "${token_symmetric_key}"
