#!/usr/bin/env ruby

require_relative "../lib/sneldb"

# Create a client
client = SnelDB::Client.new(
  base_url: "http://localhost:8085",
  user_id: "your_user_id",      # Optional
  secret_key: "your_secret_key"  # Optional
)

# Check server availability before proceeding
puts "Checking server availability..."
ping_result = client.ping
if ping_result[:success]
  puts "✅ Server is available"
else
  puts "❌ Server unavailable: #{ping_result[:error].message}"
  exit 1
end

# Define a schema
# By default, automatically grants read and write permissions to the current user
puts "Defining schema..."
client.define(
  event_type: "order_created",
  fields: {
    "id" => "int",
    "amount" => "float",
    "currency" => "string",
    "created_at" => "datetime"
  }
  # grant_permissions: true is the default - automatically grants permissions
)

# To disable automatic permission granting:
# client.define(
#   event_type: "order_created",
#   fields: { "id" => "int", "amount" => "float" },
#   grant_permissions: false
# )

# Store an event
puts "Storing event..."
client.store(
  event_type: "order_created",
  context_id: "customer-123",
  payload: {
    id: 42,
    amount: 99.99,
    currency: "USD",
    created_at: "2025-01-15T10:30:00Z"
  }
)

# Query events
puts "Querying events..."
response = client.query(
  event_type: "order_created",
  where: 'amount >= 50'
)
puts "Query result: #{response}"

# Replay events
puts "Replaying events..."
response = client.replay(context_id: "customer-123")
puts "Replay result: #{response}"

# Ping server to check availability
puts "Pinging server..."
ping_result = client.ping
if ping_result[:success]
  puts "✅ Server is responding correctly"
else
  puts "❌ Server check failed: #{ping_result[:error].message}"
end

# Or use the raising version:
begin
  client.ping!
  puts "✅ Server is available"
rescue SnelDB::ConnectionError => e
  puts "❌ Server unavailable: #{e.message}"
end

# User Management (requires admin authentication)
# First, ensure you're authenticated as admin
admin_client = SnelDB::Client.new(
  base_url: "http://localhost:8085",
  user_id: "admin",              # Admin user from config
  secret_key: "admin-key-123"   # Admin key from config
)

# Create a new user
puts "Creating user..."
result = admin_client.create_user(user_id: "api_client")
if result[:success]
  puts "User created successfully"
  # The response contains the secret key
  result[:data].each { |line| puts line[:raw] }
else
  puts "Failed to create user: #{result[:error]}"
end

# Create a user with roles
puts "Creating user with roles..."
result = admin_client.create_user(
  user_id: "editor_user",
  roles: ["editor", "read-only"]
)
if result[:success]
  puts "User with roles created successfully"
  result[:data].each { |line| puts line[:raw] }
end

# Create a user with custom key and roles
puts "Creating user with key and roles..."
result = admin_client.create_user(
  user_id: "service_account",
  secret_key: "custom-secret-key-123",
  roles: ["write-only"]
)
if result[:success]
  puts "User with key and roles created successfully"
  result[:data].each { |line| puts line[:raw] }
end

# List all users
puts "Listing users..."
users_result = admin_client.list_users
if users_result[:success]
  users_result[:data].each { |user| puts user[:raw] }
end

