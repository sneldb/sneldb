#!/usr/bin/env ruby

require_relative "../lib/sneldb"

# Create a client
client = SnelDB::Client.new(
  base_url: "http://localhost:8085",
  user_id: "your_user_id",      # Optional
  secret_key: "your_secret_key"  # Optional
)

# Define a schema
puts "Defining schema..."
client.define(
  event_type: "order_created",
  fields: {
    "id" => "int",
    "amount" => "float",
    "currency" => "string",
    "created_at" => "datetime"
  }
)

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

# Ping
puts "Pinging server..."
response = client.ping
puts "Ping result: #{response}"

