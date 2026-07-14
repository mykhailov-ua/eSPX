# Ingress for staging admin UI and Stripe webhooks. Requires an Ingress controller on the cluster.
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: espx-cold-path
  namespace: ${namespace}
  labels:
    app.kubernetes.io/part-of: espx
  annotations:
    # TLS termination typically happens at the cloud LB or cert-manager; adjust per cluster.
    nginx.ingress.kubernetes.io/proxy-body-size: "1m"
spec:
  ingressClassName: ${ingress_class}
  rules:
    - host: ${admin_host}
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: management
                port:
                  number: 8188
    - host: ${payment_webhook_host}
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: payment
                port:
                  number: 8187
  tls:
    - hosts:
        - ${admin_host}
        - ${payment_webhook_host}
      secretName: ${tls_secret_name}
