package authpage

// LoginPage is the view model for the admin login screen.
type LoginPage struct {
	CSRF    string
	Message string
	Error   string
}

// RegisterPage is the view model for the admin registration screen.
type RegisterPage struct {
	CSRF  string
	Error string
}

// FieldErrors carries validation errors for auth forms.
type FieldErrors struct {
	General string
}
