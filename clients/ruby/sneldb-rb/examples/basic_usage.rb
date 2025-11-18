#!/usr/bin/env ruby

require_relative "../lib/sneldb"

# Create a TCP client (default protocol)
# TCP is recommended for better performance
client = SnelDB::Client.new(
  address: "127.0.0.1:7171",     # TCP address format: host:port (from prod.toml)
  user_id: "admin",              # Admin user from config
  secret_key: "admin-key-123"     # Admin key from config
)

# Alternative: Use HTTP protocol
# client = SnelDB::Client.new(
#   address: "http://127.0.0.1:8085",  # HTTP URL
#   protocol: "http",
#   user_id: "admin",
#   secret_key: "admin-key-123"
# )

puts "=" * 80
puts "SnelDB Basic Usage Examples"
puts "=" * 80
puts ""

# ============================================================================
# USER MANAGEMENT
# ============================================================================

puts "\n" + "-" * 80
puts "USER MANAGEMENT"
puts "-" * 80

# Create a new user
puts "\n1. Creating a new user..."
result = client.create_user(
  user_id: "test_user",
  secret_key: "test_secret_key_123"
)
if result[:success]
  puts "   ✓ User 'test_user' created successfully"
else
  puts "   ⚠ #{result[:error].message}"
end

# List all users
puts "\n2. Listing all users..."
result = client.list_users
if result[:success]
  users = result[:data]
  if users.empty?
    puts "   No users found"
  else
  users.each do |user|
    if user.is_a?(Hash)
      user_id = user["user_id"] || user[:user_id] || user["raw"]&.split(":")&.first
      active = user["active"] || user[:active]
      raw = user["raw"] || user[:raw]
      if raw
        parts = raw.split(":")
        user_id = parts[0]&.strip if parts[0]
        active_str = parts[1]&.strip if parts[1]
        active = active_str == "active" if active_str
      end
      puts "   - #{user_id || 'unknown'}: #{active ? 'active' : 'inactive'}"
    else
      puts "   - #{user}"
    end
  end
  end
else
  puts "   ⚠ Error: #{result[:error].message}"
end

# ============================================================================
# PERMISSION MANAGEMENT
# ============================================================================

puts "\n" + "-" * 80
puts "PERMISSION MANAGEMENT"
puts "-" * 80

# Define an event type first
event_type = "order_created"
puts "\n3. Defining event schema..."
client.define!(
  event_type: event_type,
  fields: {
    "id" => "int",
    "amount" => "float",
    "currency" => "string",
    "created_at" => "datetime"
  }
  # Note: auto_grant defaults to true, so read/write permissions are automatically
  # granted to the current user after successful schema definition
)
puts "   ✓ Event schema defined (permissions automatically granted)"

# Grant permissions to the test user
# Note: When using define!, permissions are automatically granted to the defining user.
# Here we explicitly grant to test_user for demonstration.
puts "\n4. Granting permissions to 'test_user'..."
result = client.grant_permission(
  permissions: ["read", "write"],
  event_types: [event_type],
  user_id: "test_user"
)
if result[:success]
  puts "   ✓ Permissions granted to 'test_user' for '#{event_type}'"
else
  puts "   ⚠ Error: #{result[:error].message}"
end

# Show permissions for the user
puts "\n5. Showing permissions for 'test_user'..."
result = client.show_permissions(user_id: "test_user")
if result[:success]
  perms = result[:data]
  if perms.empty?
    puts "   No permissions found"
  else
  perms.each do |perm|
    if perm.is_a?(Hash)
      et = perm["event_type"] || perm[:event_type]
      p = perm["permissions"] || perm[:permissions]
      raw = perm["raw"] || perm[:raw]
      if raw
        puts "   - #{raw}"
      else
        puts "   - #{et || 'unknown'}: #{p || 'none'}"
      end
    else
      puts "   - #{perm}"
    end
  end
  end
else
  puts "   ⚠ Error: #{result[:error].message}"
end

# ============================================================================
# EVENT OPERATIONS
# ============================================================================

puts "\n" + "-" * 80
puts "EVENT OPERATIONS"
puts "-" * 80

# Store an event (using admin client)
puts "\n6. Storing event..."
result = client.store(
  event_type: event_type,
  context_id: "customer-123",
  payload: {
    id: 42,
    amount: 99.99,
    currency: "USD",
    created_at: "2025-01-15T10:30:00Z"
  }
)
if result[:success]
  puts "   ✓ Event stored successfully"
else
  puts "   ⚠ Error: #{result[:error].message}"
end

# Query events
puts "\n7. Querying events..."
result = client.query(
  event_type: event_type,
  where: 'amount >= 50'
)
if result[:success]
  events = result[:data]
  puts "   ✓ Found #{events.length} event(s)"
  events.each do |event|
    puts "     - ID: #{event['id'] || event[:id]}, Amount: #{event['amount'] || event[:amount]}"
  end
else
  puts "   ⚠ Error: #{result[:error].message}"
end

# Replay events
puts "\n8. Replaying events..."
result = client.replay(context_id: "customer-123")
if result[:success]
  events = result[:data]
  puts "   ✓ Replayed #{events.length} event(s) for context 'customer-123'"
else
  puts "   ⚠ Error: #{result[:error].message}"
end

puts "\n9. Verifying server connectivity..."
result = client.query(event_type: event_type, limit: 1)
if result[:success]
  puts "   ✓ Server is responding"
else
  puts "   ⚠ Error: #{result[:error].message}"
end

# ============================================================================
# TEST WITH AUTHENTICATED USER
# ============================================================================

puts "\n" + "-" * 80
puts "TESTING WITH AUTHENTICATED USER"
puts "-" * 80

# Create a client with the test user credentials
puts "\n10. Testing with 'test_user' credentials..."
test_client = SnelDB::Client.new(
  address: "127.0.0.1:7171",
  user_id: "test_user",
  secret_key: "test_secret_key_123"
)

# Try to store an event (should work with write permission)
result = test_client.store(
  event_type: event_type,
  context_id: "customer-456",
  payload: {
    id: 43,
    amount: 149.99,
    currency: "EUR",
    created_at: Time.now.iso8601
  }
)
if result[:success]
  puts "   ✓ Event stored successfully with 'test_user'"
else
  puts "   ⚠ Error: #{result[:error].message}"
end

# Try to query events (should work with read permission)
result = test_client.query(event_type: event_type, limit: 5)
if result[:success]
  puts "   ✓ Query successful with 'test_user' (#{result[:data].length} events)"
else
  puts "   ⚠ Error: #{result[:error].message}"
end

puts "\n" + "=" * 80
puts "Examples completed!"
puts "=" * 80
