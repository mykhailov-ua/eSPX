# Stream consumers: Redis ad:events:stream per shard to Postgres and ClickHouse.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: processor
  namespace: espx
  labels:
    app.kubernetes.io/name: processor
    app.kubernetes.io/component: cold-path
    app.kubernetes.io/part-of: espx
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: processor
  template:
    metadata:
      labels:
        app.kubernetes.io/name: processor
        app.kubernetes.io/component: cold-path
        app.kubernetes.io/part-of: espx
    spec:
      containers:
        - name: processor
          image: ad-event-processor:latest
          imagePullPolicy: IfNotPresent
          command: ["/processor"]
          envFrom:
            - configMapRef:
                name: espx-env
            - secretRef:
                name: espx-secrets
          env:
            - name: GOMEMLIMIT
              value: "1500MiB"
            - name: GOGC
              value: "100"
            - name: SERVER_PORT
              value: "8186"
          volumeMounts:
            - name: geoip
              mountPath: /deploy/geoip
              readOnly: true
          ports:
            - name: http
              containerPort: 8186
          readinessProbe:
            exec:
              command:
                - /processor
                - --health-probe
                - http://127.0.0.1:8186/health
            initialDelaySeconds: 15
            periodSeconds: 10
          resources:
            requests:
              memory: 256Mi
              cpu: 200m
            limits:
              memory: 2Gi
      volumes:
        - name: geoip
          hostPath:
            path: ${geoip_host_path}
            type: DirectoryOrCreate
