package ingestion

func fraudBoostsFromWatcher(watcher *SettingsWatcher) *FraudScoreBoostSnapshot {
	if watcher == nil {
		return nil
	}
	return watcher.GetFraudScoreBoosts()
}
