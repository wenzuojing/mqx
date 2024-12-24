package config

type Config struct {
	// Server
	Port string `env:"PORT" envDefault:"8080"`

	// Database
	DatabaseURL string `env:"DATABASE_URL" envDefault:"postgres://postgres:postgres@localhost:5432/postgres"`

	// JWT
	JWTSecret string `env:"JWT_SECRET" envDefault:"secret"`
}
