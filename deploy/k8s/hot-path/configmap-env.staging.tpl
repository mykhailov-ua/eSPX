# Staging hot-path env for tracker x4 against external managed Redis (espx-edge namespace).
apiVersion: v1
kind: ConfigMap
metadata:
  name: espx-edge-env
  namespace: espx-edge
  labels:
    app.kubernetes.io/part-of: espx
    app.kubernetes.io/component: hot-path
data:
  ENV: "${env}"
  REDIS_SHARD_COUNT: "4"
  REDIS_ADDRS: "${redis_addrs}"
  FILTER_TIMEOUT_MS: "${filter_timeout_ms}"
  GEOIP_DB_PATH: "/deploy/geoip/GeoLite2-Country.mmdb"
  GEOIP_UPDATER_ENABLED: "false"
  RTB_MODE: "off"
  RTB_BUDGET_AUTHORITY: "redis"
  # M1–M2 sharding hot-path (staging rollout)
  MIGRATION_FENCE_ENABLED: "true"
  UDP_CONTROL_ENABLED: "true"
  UDP_FAIL_CLOSED: "true"
  UDP_MGMT_ADDR: "${udp_mgmt_addr}"
  UDP_SYNC_INTERVAL_MS: "10000"
  UDP_DEFAULT_SHARD_RPS: "50000"
