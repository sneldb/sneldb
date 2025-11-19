#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative "../lib/sneldb"

puts "=" * 80
puts "Real-World SnelDB Scenario: TCP Connection with Role-Based Access"
puts "=" * 80
puts

# ============================================================================
# Step 1: Connect as Admin via TCP
# ============================================================================
puts "Step 1: Connecting to SnelDB via TCP as admin..."
admin_client = SnelDB::Client.new(
  base_url: "tcp://localhost:7171",
  user_id: "admin",
  secret_key: "admin-key-123"
)

# Check server availability
begin
  admin_client.ping!
  puts "✅ Server is available"
rescue SnelDB::ConnectionError => e
  puts "❌ Cannot connect to server: #{e.message}"
  puts "   Make sure SnelDB is running on tcp://localhost:7171"
  exit 1
end

# Authenticate for TCP (establishes session token)
begin
  token = admin_client.authenticate!
  puts "✅ Authenticated as admin (session token: #{token[0..10]}...)"
rescue SnelDB::AuthenticationError => e
  puts "❌ Authentication failed: #{e.message}"
  exit 1
end

# ============================================================================
# Step 2: Create a Read-Only User
# ============================================================================
puts "\nStep 2: Creating read-only user..."
readonly_user_id = "analyst_user_#{Time.now.to_i}"
readonly_secret_key = "analyst-secret-key-#{rand(10000)}"

result = admin_client.create_user(
  user_id: readonly_user_id,
  secret_key: readonly_secret_key,
  roles: ["read-only"]
)

if result[:success]
  puts "✅ Read-only user '#{readonly_user_id}' created successfully"
  puts "   Secret key: #{readonly_secret_key}"
  result[:data].each { |line| puts "   #{line[:raw]}" }
else
  puts "❌ Failed to create user: #{result[:error].message}"
  exit 1
end

# ============================================================================
# Step 3: Admin Defines Event Types
# ============================================================================
puts "\nStep 3: Admin defining event types..."

event_types = [
  {
    name: "order_created",
    fields: {
      "order_id" => "int",
      "customer_id" => "int",
      "amount" => "float",
      "currency" => "string",
      "status" => "string",
      "created_at" => "datetime"
    }
  },
  {
    name: "payment_processed",
    fields: {
      "payment_id" => "int",
      "order_id" => "int",
      "amount" => "float",
      "payment_method" => "string",
      "processed_at" => "datetime"
    }
  },
  {
    name: "order_shipped",
    fields: {
      "order_id" => "int",
      "tracking_number" => "string",
      "carrier" => "string",
      "shipped_at" => "datetime"
    }
  }
]

event_types.each do |event_type|
  result = admin_client.define(
    event_type: event_type[:name],
    fields: event_type[:fields]
    # grant_permissions: true is default - admin gets permissions automatically
  )

  if result[:success]
    puts "✅ Defined event type: #{event_type[:name]}"
  else
    puts "❌ Failed to define #{event_type[:name]}: #{result[:error].message}"
  end
end

# ============================================================================
# Step 4: Admin Grants Permissions to Read-Only User
# ============================================================================
puts "\nStep 4: Granting read permissions to read-only user..."

event_types.each do |event_type|
  result = admin_client.grant_permission(
    user_id: readonly_user_id,
    permissions: ["read"],
    event_types: [event_type[:name]]
  )

  if result[:success]
    puts "✅ Granted read permission on #{event_type[:name]} to #{readonly_user_id}"
  else
    puts "⚠️  Failed to grant permission: #{result[:error].message}"
  end
end

# ============================================================================
# Step 5: Admin Stores Events
# ============================================================================
puts "\nStep 5: Admin storing sample events..."

orders = [
  {
    order_id: 1001,
    customer_id: 501,
    amount: 149.99,
    currency: "USD",
    status: "confirmed",
    created_at: "2025-01-15T10:30:00Z"
  },
  {
    order_id: 1002,
    customer_id: 502,
    amount: 79.50,
    currency: "EUR",
    status: "confirmed",
    created_at: "2025-01-15T11:15:00Z"
  },
  {
    order_id: 1003,
    customer_id: 501,
    amount: 299.99,
    currency: "USD",
    status: "pending",
    created_at: "2025-01-15T12:00:00Z"
  }
]

orders.each do |order|
  result = admin_client.store(
    event_type: "order_created",
    context_id: "customer-#{order[:customer_id]}",
    payload: order
  )

  if result[:success]
    puts "✅ Stored order_created event for order #{order[:order_id]}"
  else
    puts "❌ Failed to store order #{order[:order_id]}: #{result[:error].message}"
  end
end

# Store payment events
payments = [
  {
    payment_id: 2001,
    order_id: 1001,
    amount: 149.99,
    payment_method: "credit_card",
    processed_at: "2025-01-15T10:35:00Z"
  },
  {
    payment_id: 2002,
    order_id: 1002,
    amount: 79.50,
    payment_method: "paypal",
    processed_at: "2025-01-15T11:20:00Z"
  }
]

payments.each do |payment|
  result = admin_client.store(
    event_type: "payment_processed",
    context_id: "customer-#{orders.find { |o| o[:order_id] == payment[:order_id] }[:customer_id]}",
    payload: payment
  )

  if result[:success]
    puts "✅ Stored payment_processed event for payment #{payment[:payment_id]}"
  else
    puts "❌ Failed to store payment #{payment[:payment_id]}: #{result[:error].message}"
  end
end

# Store shipping event
result = admin_client.store(
  event_type: "order_shipped",
  context_id: "customer-501",
  payload: {
    order_id: 1001,
    tracking_number: "TRACK123456",
    carrier: "UPS",
    shipped_at: "2025-01-16T09:00:00Z"
  }
)

if result[:success]
  puts "✅ Stored order_shipped event for order 1001"
end

# ============================================================================
# Step 6: Switch to Read-Only User
# ============================================================================
puts "\nStep 6: Switching to read-only user for queries..."

# Close admin connection
admin_client.close

# Create new client with read-only user credentials
readonly_client = SnelDB::Client.new(
  base_url: "tcp://localhost:7171",
  user_id: readonly_user_id,
  secret_key: readonly_secret_key
)

# Authenticate read-only user
begin
  token = readonly_client.authenticate!
  puts "✅ Authenticated as #{readonly_user_id} (session token: #{token[0..10]}...)"
rescue SnelDB::AuthenticationError => e
  puts "❌ Authentication failed: #{e.message}"
  exit 1
end

# ============================================================================
# Step 7: Read-Only User Queries Events
# ============================================================================
puts "\nStep 7: Read-only user querying events..."

# Query all orders
puts "\nQuerying all orders:"
result = readonly_client.query(
  event_type: "order_created",
  limit: 10
)

if result[:success] && result[:data]
  result[:data].each do |row|
    if row.is_a?(Hash) && row["order_id"]
      puts "  Order #{row["order_id"]}: $#{row["amount"]} #{row["currency"]} - #{row["status"]}"
    elsif row[:raw]
      puts "  #{row[:raw]}"
    end
  end
else
  puts "  No orders found or query failed"
end

# Query orders with filter
puts "\nQuerying orders with amount > 100:"
result = readonly_client.query(
  event_type: "order_created",
  where: 'amount > 100',
  limit: 10
)

if result[:success] && result[:data]
  result[:data].each do |row|
    if row.is_a?(Hash) && row["order_id"]
      puts "  Order #{row["order_id"]}: $#{row["amount"]} #{row["currency"]}"
    elsif row[:raw]
      puts "  #{row[:raw]}"
    end
  end
end

# Query payments
puts "\nQuerying all payments:"
result = readonly_client.query(
  event_type: "payment_processed",
  limit: 10
)

if result[:success] && result[:data]
  result[:data].each do |row|
    if row.is_a?(Hash) && row["payment_id"]
      puts "  Payment #{row["payment_id"]}: $#{row["amount"]} via #{row["payment_method"]}"
    elsif row[:raw]
      puts "  #{row[:raw]}"
    end
  end
end

# ============================================================================
# Step 8: Read-Only User Replays Events for a Customer
# ============================================================================
puts "\nStep 8: Read-only user replaying events for customer-501..."

result = readonly_client.replay(
  context_id: "customer-501"
)

if result[:success] && result[:data]
  puts "\nEvent timeline for customer-501:"
  result[:data].each_with_index do |row, index|
    if row.is_a?(Hash)
      event_type = row["event_type"] || "unknown"
      puts "  #{index + 1}. #{event_type}"
      row.each do |key, value|
        next if key == "event_type"
        puts "     #{key}: #{value}"
      end
    elsif row[:raw]
      puts "  #{index + 1}. #{row[:raw]}"
    end
  end
else
  puts "  No events found or replay failed"
end

# ============================================================================
# Step 9: Demonstrate Read-Only Restrictions
# ============================================================================
puts "\nStep 9: Demonstrating read-only restrictions..."

# Try to define an event type (should fail)
puts "\nAttempting to define event type (should fail for read-only user)..."
result = readonly_client.define(
  event_type: "test_event",
  fields: { "id" => "int" }
)

if result[:success]
  puts "⚠️  Unexpected: Define succeeded (this shouldn't happen for read-only user)"
else
  puts "✅ Correctly prevented: #{result[:error].class} - #{result[:error].message}"
end

# Try to store an event (should fail)
puts "\nAttempting to store event (should fail for read-only user)..."
result = readonly_client.store(
  event_type: "order_created",
  context_id: "customer-999",
  payload: { order_id: 9999, amount: 50.0 }
)

if result[:success]
  puts "⚠️  Unexpected: Store succeeded (this shouldn't happen for read-only user)"
else
  puts "✅ Correctly prevented: #{result[:error].class} - #{result[:error].message}"
end

# ============================================================================
# Cleanup
# ============================================================================
puts "\n" + "=" * 80
puts "Cleaning up..."
readonly_client.close
puts "✅ Connections closed"

puts "\n" + "=" * 80
puts "Scenario Complete!"
puts "=" * 80
puts "\nSummary:"
puts "  - Connected via TCP as admin"
puts "  - Created read-only user '#{readonly_user_id}'"
puts "  - Admin defined #{event_types.length} event types"
puts "  - Admin stored #{orders.length + payments.length + 1} events"
puts "  - Read-only user queried events successfully"
puts "  - Read-only user replayed events for a customer"
puts "  - Read-only restrictions verified (cannot define/store)"
puts

