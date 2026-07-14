# Dev-only credentials. Replace before staging; never commit real secrets to git.
apiVersion: v1
kind: Secret
metadata:
  name: espx-secrets
  namespace: espx
  labels:
    app.kubernetes.io/part-of: espx
type: Opaque
stringData:
  DB_DSN: "postgres://ad_event_processor_user:secure_pass_123@${host_ip}:5430/ad_event_processor?sslmode=disable"
  PAYMENT_DB_DSN: "postgres://espx_payment_user:secure_payment_pass_123@${host_ip}:5431/espx_payment?sslmode=disable"
  CH_DSN: "clickhouse://default:secure_ch_pass@${host_ip}:9000/ad_event_processor"
  REDIS_PASSWORD: "your_redis_password_here"
  PAYMENT_INTERNAL_TOKEN: "dev-payment-internal-token"
  SETTLEMENT_INTERNAL_TOKEN: "dev-settlement-internal-token"
  BILLING_INTERNAL_TOKEN: "dev-billing-internal-token"
  TOKEN_SYMMETRIC_KEY: "01234567890123456789012345678901"
  ADMIN_API_KEY: "dev-admin-api-key-change-me"
  STRIPE_SECRET_KEY: ""
  STRIPE_WEBHOOK_SECRET: ""
  TELEGRAM_BOT_TOKEN: ""
  TELEGRAM_CHAT_ID: ""
  MAXMIND_LICENSE_KEY: ""
