# SnelDB Configuration
# Configure your SnelDB connection here

Rails.application.config.sneldb = {
  base_url: ENV.fetch("SNELDB_URL", "http://localhost:8085"),
  user_id: ENV["SNELDB_USER_ID"],
  secret_key: ENV["SNELDB_SECRET_KEY"],
  output_format: ENV.fetch("SNELDB_OUTPUT_FORMAT", "text")
}

