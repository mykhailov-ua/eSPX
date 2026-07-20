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

[Service]
Type=simple
EnvironmentFile=-/etc/espx/secrets.env
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
		"Interface": profile.Interface,
		"Profile":   string(profile.Type),
	}
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, err
	}
	if profile.EdgeXDP {
		buf.WriteString(fmt.Sprintf("\n# edge_xdp enabled on %s\n", profile.Interface))
	}
	return buf.Bytes(), nil
}
