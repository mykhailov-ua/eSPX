# Four gnet trackers on host ports 8181-8184. hostNetwork avoids pod CNI latency on /track.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tracker-0
  namespace: espx-edge
  labels:
    app.kubernetes.io/name: tracker-0
    app.kubernetes.io/component: hot-path
    app.kubernetes.io/part-of: espx
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/name: tracker-0
  template:
    metadata:
      labels:
        app.kubernetes.io/name: tracker-0
        app.kubernetes.io/component: hot-path
    spec:
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      containers:
        - name: tracker
          image: ad-event-processor:latest
          imagePullPolicy: IfNotPresent
          command: ["/tracker"]
          envFrom:
            - configMapRef:
                name: espx-edge-env
            - secretRef:
                name: espx-edge-secrets
          env:
            - name: SERVER_PORT
              value: "8181"
            - name: UDP_TRACKER_ID
              value: "1"
            - name: GOMEMLIMIT
              value: "700MiB"
            - name: GOGC
              value: "50"
          volumeMounts:
            - name: geoip
              mountPath: /deploy/geoip
              readOnly: true
            - name: logs
              mountPath: /var/log/espx
          livenessProbe:
            exec:
              command:
                - /tracker
                - --health-probe
                - http://127.0.0.1:8181/healthz
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            exec:
              command:
                - /tracker
                - --health-probe
                - http://127.0.0.1:8181/readyz
            initialDelaySeconds: 10
            periodSeconds: 10
          resources:
            requests:
              memory: 256Mi
              cpu: 500m
            limits:
              memory: 1Gi
      volumes:
        - name: geoip
          hostPath:
            path: ${geoip_host_path}
            type: DirectoryOrCreate
        - name: logs
          hostPath:
            path: /var/lib/espx/logs
            type: DirectoryOrCreate
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tracker-1
  namespace: espx-edge
  labels:
    app.kubernetes.io/name: tracker-1
    app.kubernetes.io/component: hot-path
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/name: tracker-1
  template:
    metadata:
      labels:
        app.kubernetes.io/name: tracker-1
        app.kubernetes.io/component: hot-path
    spec:
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      containers:
        - name: tracker
          image: ad-event-processor:latest
          imagePullPolicy: IfNotPresent
          command: ["/tracker"]
          envFrom:
            - configMapRef:
                name: espx-edge-env
            - secretRef:
                name: espx-edge-secrets
          env:
            - name: SERVER_PORT
              value: "8182"
            - name: UDP_TRACKER_ID
              value: "2"
            - name: GOMEMLIMIT
              value: "700MiB"
            - name: GOGC
              value: "50"
          volumeMounts:
            - name: geoip
              mountPath: /deploy/geoip
              readOnly: true
            - name: logs
              mountPath: /var/log/espx
          livenessProbe:
            exec:
              command:
                - /tracker
                - --health-probe
                - http://127.0.0.1:8182/healthz
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            exec:
              command:
                - /tracker
                - --health-probe
                - http://127.0.0.1:8182/readyz
            initialDelaySeconds: 10
            periodSeconds: 10
          resources:
            limits:
              memory: 1Gi
      volumes:
        - name: geoip
          hostPath:
            path: ${geoip_host_path}
            type: DirectoryOrCreate
        - name: logs
          hostPath:
            path: /var/lib/espx/logs
            type: DirectoryOrCreate
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tracker-2
  namespace: espx-edge
  labels:
    app.kubernetes.io/name: tracker-2
    app.kubernetes.io/component: hot-path
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/name: tracker-2
  template:
    metadata:
      labels:
        app.kubernetes.io/name: tracker-2
        app.kubernetes.io/component: hot-path
    spec:
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      containers:
        - name: tracker
          image: ad-event-processor:latest
          imagePullPolicy: IfNotPresent
          command: ["/tracker"]
          envFrom:
            - configMapRef:
                name: espx-edge-env
            - secretRef:
                name: espx-edge-secrets
          env:
            - name: SERVER_PORT
              value: "8183"
            - name: UDP_TRACKER_ID
              value: "3"
            - name: GOMEMLIMIT
              value: "700MiB"
            - name: GOGC
              value: "50"
          volumeMounts:
            - name: geoip
              mountPath: /deploy/geoip
              readOnly: true
            - name: logs
              mountPath: /var/log/espx
          livenessProbe:
            exec:
              command:
                - /tracker
                - --health-probe
                - http://127.0.0.1:8183/healthz
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            exec:
              command:
                - /tracker
                - --health-probe
                - http://127.0.0.1:8183/readyz
            initialDelaySeconds: 10
            periodSeconds: 10
          resources:
            limits:
              memory: 1Gi
      volumes:
        - name: geoip
          hostPath:
            path: ${geoip_host_path}
            type: DirectoryOrCreate
        - name: logs
          hostPath:
            path: /var/lib/espx/logs
            type: DirectoryOrCreate
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tracker-3
  namespace: espx-edge
  labels:
    app.kubernetes.io/name: tracker-3
    app.kubernetes.io/component: hot-path
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/name: tracker-3
  template:
    metadata:
      labels:
        app.kubernetes.io/name: tracker-3
        app.kubernetes.io/component: hot-path
    spec:
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      containers:
        - name: tracker
          image: ad-event-processor:latest
          imagePullPolicy: IfNotPresent
          command: ["/tracker"]
          envFrom:
            - configMapRef:
                name: espx-edge-env
            - secretRef:
                name: espx-edge-secrets
          env:
            - name: SERVER_PORT
              value: "8184"
            - name: UDP_TRACKER_ID
              value: "4"
            - name: GOMEMLIMIT
              value: "700MiB"
            - name: GOGC
              value: "50"
          volumeMounts:
            - name: geoip
              mountPath: /deploy/geoip
              readOnly: true
            - name: logs
              mountPath: /var/log/espx
          livenessProbe:
            exec:
              command:
                - /tracker
                - --health-probe
                - http://127.0.0.1:8184/healthz
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            exec:
              command:
                - /tracker
                - --health-probe
                - http://127.0.0.1:8184/readyz
            initialDelaySeconds: 10
            periodSeconds: 10
          resources:
            limits:
              memory: 1Gi
      volumes:
        - name: geoip
          hostPath:
            path: ${geoip_host_path}
            type: DirectoryOrCreate
        - name: logs
          hostPath:
            path: /var/lib/espx/logs
            type: DirectoryOrCreate
