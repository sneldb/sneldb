# SnelDB Configuration
# Configure your SnelDB connection here

Rails.application.config.sneldb = {
  address: ENV.fetch("SNELDB_URL", "http://localhost:8085"),
  protocol: ENV["SNELDB_PROTOCOL"],  # Optional: "tcp" or "http" (auto-detected from address if not set)
  user_id: ENV["SNELDB_USER_ID"],
  secret_key: ENV["SNELDB_SECRET_KEY"],
  output_format: ENV.fetch("SNELDB_OUTPUT_FORMAT", "text")
}

