package rtb

const (
	geoShardCount       = 64
	geoShardMask        = geoShardCount - 1
	legacyGeoShardCount = 16
)

// CampaignAuctionRegistry stores each targeting dimension in parallel slices so shard scans stay sequential in memory.
type CampaignAuctionRegistry struct {
	Count                 int
	CampaignIDs           []CampaignID
	Bids                  []int64
	CTRPPM                []uint32
	Reserves              []int64
	DailyBudgets          []int64
	PacingOpen            []uint8
	DeviceMasks           []uint8
	CategoryMasks         []uint64
	GeoHashes             []uint32
	Weights               []uint32
	BoostPPM              []uint32
	BudgetIndices         []uint32
	CustomerBudgetIndices []uint32

	GeoBucketCount int
	GeoBucketHash  []uint32
	GeoBucketStart []uint32
	GeoBucketSoA   candidateBucketSoA

	TargetBucketCount int
	TargetBucketKey   []uint64
	TargetBucketStart []uint32
	TargetBucketSoA   candidateBucketSoA
}

// CampaignData is the cold-path input shape used when management sync rebuilds auction shards.
type CampaignData struct {
	ID             CampaignID
	Bid            int64
	CTRPPM         uint32
	Reserve        int64
	DailyBudget    int64
	PacingOpen     uint8
	CustomerID     CustomerID
	CustomerBudget int64
	DeviceMask     uint8
	CategoryMask   uint64
	GeoHashVal     uint32
	Weight         uint32
	BoostPPM       uint32
	Budget         int64
}

// catalogSnapshot holds every geo shard so UpdateCampaigns publishes the full catalog in one atomic store.
type catalogSnapshot struct {
	shards [geoShardCount]*CampaignAuctionRegistry
}
