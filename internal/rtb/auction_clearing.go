package rtb

// ClearingMode selects how the winner price is computed after ranking.
type ClearingMode uint8

const (
	// ClearingSecondPrice charges the runner-up bid when competition requires it.
	ClearingSecondPrice ClearingMode = iota
	// ClearingFirstPrice charges the winner max bid capped by reserve and publisher floor.
	ClearingFirstPrice
)

func (registry *Registry) clearingPrice(mode ClearingMode, floor int64, winnerBid int64, secondBid int64) int64 {
	if mode == ClearingFirstPrice {
		return winnerBid
	}
	price := floor
	if secondBid != -1 && secondBid > price {
		price = secondBid
	}
	return price
}

func applyReserve(price int64, reserve int64, winnerBid int64) int64 {
	if reserve > price {
		price = reserve
	}
	if price > winnerBid {
		price = winnerBid
	}
	return price
}
