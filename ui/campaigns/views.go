package campaigns

// Row is one campaign in the list table.
type Row struct {
	ID           string
	Name         string
	Status       string
	BudgetLimit  string
	CurrentSpend string
	PacingMode   string
	CustomerID   string
}

// CustomerOption is a customer select option on the create form.
type CustomerOption struct {
	ID   string
	Name string
}

// ListPage is the campaigns index view model.
type ListPage struct {
	CSRF       string
	UserEmail  string
	UserRole   string
	GrafanaURL string
	Campaigns  []Row
	Total      int64
	FlashOK    string
	FlashError string
}

// NewPage is the create-campaign form view model.
type NewPage struct {
	CSRF       string
	UserEmail  string
	UserRole   string
	GrafanaURL string
	Customers  []CustomerOption
	Error      string
	Form       NewForm
}

// NewForm holds repopulated create-campaign field values after validation errors.
type NewForm struct {
	CustomerID      string
	Name            string
	BudgetLimit     string
	PacingMode      string
	Timezone        string
	TargetCountries string
}
