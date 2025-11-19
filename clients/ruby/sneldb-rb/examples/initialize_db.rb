#!/usr/bin/env ruby

require_relative "../lib/sneldb"

# Example 1: Auto-initialize on client creation
# This will automatically check if the database is initialized and create
# the admin user if needed, using credentials from config (prod.toml)
client = SnelDB::Client.new(
  base_url: "http://localhost:8085",
  auto_initialize: true,
  initial_admin_user: "admin",        # From config: initial_admin_user
  initial_admin_key: "admin-key-123" # From config: initial_admin_key
)

# Example 2: Manual initialization check
client2 = SnelDB::Client.new(
  base_url: "http://localhost:8085"
)

# Check if database is initialized
if !client2.initialized?
  puts "Database not initialized, initializing..."
  client2.ensure_initialized!(
    admin_user_id: "admin",
    admin_secret_key: "admin-key-123"
  )
  puts "Database initialized!"
else
  puts "Database already initialized"
end

# Example 3: Use initialized client
# After initialization, you can use the client normally
client.define(
  event_type: "order_created",
  fields: {
    "id" => "int",
    "amount" => "float"
  }
)

puts "Schema defined successfully!"

