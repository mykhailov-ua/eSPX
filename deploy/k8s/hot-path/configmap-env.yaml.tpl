# Tracker env shared by tracker-0..3. SERVER_PORT is set per Deployment.
apiVersion: v1
kind: ConfigMap
metadata:
  name: espx-edge-env
  namespace: espx-edge
  labels:
    app.kubernetes.io/part-of: espx
    app.kubernetes.io/component: hot-path
data:
  ENV: "development"
  REDIS_SHARD_COUNT: "4"
  REDIS_ADDRS: "${host_ip}:6479,${host_ip}:6480,${host_ip}:6481,${host_ip}:6482"
  FILTER_TIMEOUT_MS: "5000"
  GEOIP_DB_PATH: "/deploy/geoip/GeoLite2-Country.mmdb"
  GEOIP_UPDATER_ENABLED: "false"
  RTB_MODE: "off"
  RTB_BUDGET_AUTHORITY: "redis"
