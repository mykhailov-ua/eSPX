package ingestion

// parseTrackJSONLegacy is the pre-opt bool-chain parser kept for benchmark comparison.
func parseTrackJSONLegacy(v *TrackRequest, data []byte) error {
	v.Reset()
	if len(data) == 0 {
		return errMalformedJSON
	}

	_ = data[len(data)-1]

	n := len(data)
	i := 0

	for i < n && (data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r') {
		i++
	}

	if i >= n || data[i] != '{' {
		return errMalformedJSON
	}
	i++

	for i < n {
		for i < n && (data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r') {
			i++
		}
		if i >= n {
			return errMalformedJSON
		}

		if data[i] == '}' {
			return nil
		}

		if data[i] != '"' {
			return errMalformedJSON
		}
		i++

		keyStart := i
		for i < n && data[i] != '"' {
			if data[i] == '\\' {
				return errMalformedJSON
			}
			i++
		}
		if i >= n {
			return errMalformedJSON
		}
		keyEnd := i
		i++

		key := data[keyStart:keyEnd]

		for i < n && (data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r') {
			i++
		}
		if i >= n || data[i] != ':' {
			return errMalformedJSON
		}
		i++

		for i < n && (data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r') {
			i++
		}
		if i >= n {
			return errMalformedJSON
		}

		isCampaignID := false
		isUserID := false
		isType := false
		isClickID := false
		isPayload := false
		isPlacementID := false

		switch len(key) {
		case 4:
			if key[0] == 't' && key[1] == 'y' && key[2] == 'p' && key[3] == 'e' {
				isType = true
			}
		case 7:
			if key[0] == 'p' && key[1] == 'a' && key[2] == 'y' && key[3] == 'l' && key[4] == 'o' && key[5] == 'a' && key[6] == 'd' {
				isPayload = true
			} else if key[0] == 'u' && key[1] == 's' && key[2] == 'e' && key[3] == 'r' && key[4] == '_' && key[5] == 'i' && key[6] == 'd' {
				isUserID = true
			}
		case 8:
			if key[0] == 'c' && key[1] == 'l' && key[2] == 'i' && key[3] == 'c' && key[4] == 'k' && key[5] == '_' && key[6] == 'i' && key[7] == 'd' {
				isClickID = true
			}
		case 11:
			if key[0] == 'c' && key[1] == 'a' && key[2] == 'm' && key[3] == 'p' && key[4] == 'a' && key[5] == 'i' && key[6] == 'g' && key[7] == 'n' && key[8] == '_' && key[9] == 'i' && key[10] == 'd' {
				isCampaignID = true
			}
		case 12:
			if key[0] == 'p' && key[1] == 'l' && key[2] == 'a' && key[3] == 'c' && key[4] == 'e' && key[5] == 'm' && key[6] == 'e' && key[7] == 'n' && key[8] == 't' && key[9] == '_' && key[10] == 'i' && key[11] == 'd' {
				isPlacementID = true
			}
		}

		if isCampaignID || isUserID || isType || isClickID || isPlacementID {
			if data[i] != '"' {
				return errMalformedJSON
			}
			i++
			valStart := i
			for i < n && data[i] != '"' {
				if data[i] == '\\' {
					i += 2
				} else {
					i++
				}
			}
			if i >= n {
				return errMalformedJSON
			}
			valEnd := i
			i++

			valBytes := data[valStart:valEnd]
			if isCampaignID {
				if !ParseUUID(valBytes, &v.CampaignID) {
					return errMalformedJSON
				}
			} else if isUserID {
				v.UserID = unsafeString(valBytes)
			} else if isType {
				v.Type = unsafeString(valBytes)
			} else if isClickID {
				v.ClickID = unsafeString(valBytes)
			} else if isPlacementID {
				v.PlacementID = unsafeString(valBytes)
			}
		} else if isPayload {
			valStart := i
			valEnd, err := skipJSONValue(data, i)
			if err != nil {
				return err
			}
			v.Payload = data[valStart:valEnd]
			i = valEnd
		} else {
			valEnd, err := skipJSONValue(data, i)
			if err != nil {
				return err
			}
			i = valEnd
		}

		for i < n && (data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r') {
			i++
		}
		if i >= n {
			return errMalformedJSON
		}

		if data[i] == ',' {
			i++
			continue
		} else if data[i] == '}' {
			return nil
		}
		return errMalformedJSON
	}

	return errMalformedJSON
}
