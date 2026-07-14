# Staging secrets: inject via terraform.tfvars or CI secret store (never commit real values).
apiVersion: v1
kind: Secret
metadata:
  name: espx-secrets
  namespace: ${namespace}
  labels:
    app.kubernetes.io/part-of: espx
type: Opaque
stringData:
  DB_DSN: "${db_dsn}"
  PAYMENT_DB_DSN: "${payment_db_dsn}"
  CH_DSN: "${ch_dsn}"
  REDIS_PASSWORD: "${redis_password}"
  PAYMENT_INTERNAL_TOKEN: "${payment_internal_token}"
  SETTLEMENT_INTERNAL_TOKEN: "${settlement_internal_token}"
  BILLING_INTERNAL_TOKEN: "${billing_internal_token}"
  TOKEN_SYMMETRIC_KEY: "${token_symmetric_key}"
  ADMIN_API_KEY: "${admin_api_key}"
  STRIPE_SECRET_KEY: "${stripe_secret_key}"
  STRIPE_WEBHOOK_SECRET: "${stripe_webhook_secret}"
  TELEGRAM_BOT_TOKEN: "${telegram_bot_token}"
  TELEGRAM_CHAT_ID: "${telegram_chat_id}"
  MAXMIND_LICENSE_KEY: "${maxmind_license_key}"
