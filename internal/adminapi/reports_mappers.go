package adminapi

// placementReportCHRow is one ClickHouse row from placement stats (M6 CHG).
type placementReportCHRow struct {
	PlacementID  string
	CampaignID   string
	Impressions  int64
	Clicks       int64
	Conversions  int64
	SpendMicro   int64
	RevenueMicro int64
}

// keywordReportCHRow is one ClickHouse row from keyword stats (M6 CHG).
type keywordReportCHRow struct {
	Keyword      string
	CampaignID   string
	Impressions  int64
	Clicks       int64
	Conversions  int64
	SpendMicro   int64
	RevenueMicro int64
}

func toPlacementReportRowDTO(row placementReportCHRow) PlacementReportRowDTO {
	profit := row.RevenueMicro - row.SpendMicro
	var roi float64
	if row.SpendMicro > 0 {
		roi = float64(profit) / float64(row.SpendMicro) * 100
	}
	var cpa int64
	if row.Conversions > 0 {
		cpa = row.SpendMicro / row.Conversions
	}
	return PlacementReportRowDTO{
		PlacementID:  row.PlacementID,
		CampaignID:   row.CampaignID,
		Impressions:  row.Impressions,
		Clicks:       row.Clicks,
		Conversions:  row.Conversions,
		SpendMicro:   row.SpendMicro,
		RevenueMicro: row.RevenueMicro,
		ProfitMicro:  profit,
		ROIPct:       roi,
		CPAMicro:     cpa,
	}
}

func toKeywordReportRowDTO(row keywordReportCHRow) KeywordReportRowDTO {
	profit := row.RevenueMicro - row.SpendMicro
	var roi float64
	if row.SpendMicro > 0 {
		roi = float64(profit) / float64(row.SpendMicro) * 100
	}
	return KeywordReportRowDTO{
		Keyword:      row.Keyword,
		CampaignID:   row.CampaignID,
		Impressions:  row.Impressions,
		Clicks:       row.Clicks,
		Conversions:  row.Conversions,
		SpendMicro:   row.SpendMicro,
		RevenueMicro: row.RevenueMicro,
		ProfitMicro:  profit,
		ROIPct:       roi,
	}
}
