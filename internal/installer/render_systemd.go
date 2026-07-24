package installer

import (
	"bytes"
	"fmt"
	"text/template"
)

const trackerUnitTemplate = `[Unit]
Description=eSPX Tracker
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=10
StartLimitBurst=3
OnFailure=espx-rollback@tracker.service

[Service]
Type=simple
EnvironmentFile=-/etc/espx/secrets.env
Environment=TRACKER_INGRESS_SCHEMA={{.IngressSchema}}
Environment=GOGC={{.GOGC}}
Environment=GOMEMLIMIT={{.GOMEMLIMIT}}
ExecStart=/usr/local/bin/tracker
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
`

func renderSystemdUnit(profile *InstallProfile) ([]byte, error) {
	tmpl, err := template.New("tracker").Parse(trackerUnitTemplate)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	data := map[string]string{
		"Interface":     profile.Interface,
		"Profile":       string(profile.Type),
		"IngressSchema": string(profile.IngressSchema),
		"GOGC":          trackerGOGC,
		"GOMEMLIMIT":    trackerGOMEMLIMIT,
	}
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, err
	}
	if profile.EdgeXDP {
		buf.WriteString(fmt.Sprintf("\n# edge_xdp enabled on %s\n", profile.Interface))
	}
	return buf.Bytes(), nil
}
