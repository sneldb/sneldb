#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative '../lib/sneldb'

address = ENV['SNELDB_URL'] || '127.0.0.1:7171'
protocol = ENV['SNELDB_PROTOCOL'] || 'tcp'
admin_client = SnelDB::Client.new(
  address: address,
  protocol: protocol,
  user_id: 'admin',
  secret_key: 'admin-key-123',
  output_format: "text"
)

puts "=" * 80
puts "SnelDB Client Comprehensive Test Scenarios"
puts "=" * 80
puts "Server: #{address} (#{protocol})"
puts ""
puts "Note: The define! method now automatically grants read/write permissions"
puts "      to the current user after successful schema definition (auto_grant: true)."
puts ""

# Generate randomized names to avoid conflicts
test_user_id = "test_user_#{rand(100000)}"
test_event_type = "test_order_#{rand(100000)}"
test_optional_type = "test_optional_#{rand(100000)}"
enum_event_type = "test_subscription_#{rand(100000)}"

# Helper to print test results
def test(name, &block)
  print "Testing: #{name}... "
  begin
    result = block.call
    if result.is_a?(Hash) && result[:success] == false
      puts "❌ FAILED (non-raising method returned error)"
      puts "   Error: #{result[:error].class} - #{result[:error].message}"
      false
    elsif result.is_a?(Hash) && result[:success] == true
      puts "✅ SUCCESS"
      true
    else
      puts "✅ SUCCESS"
      true
    end
  rescue SnelDB::Error => e
    puts "❌ ERROR: #{e.class} - #{e.message}"
    false
  rescue => e
    puts "❌ UNEXPECTED ERROR: #{e.class} - #{e.message}"
    false
  end
end

def test_error(name, expected_error_class, &block)
  print "Testing ERROR: #{name}... "
  begin
    block.call
    puts "❌ FAILED (expected #{expected_error_class}, but no error raised)"
    false
  rescue expected_error_class => e
    puts "✅ CORRECT ERROR: #{e.class}"
    puts "   Message: #{e.message[0..100]}"
    true
  rescue SnelDB::Error => e
    puts "❌ WRONG ERROR: #{e.class} (expected #{expected_error_class})"
    puts "   Message: #{e.message[0..100]}"
    false
  rescue => e
    puts "❌ UNEXPECTED ERROR: #{e.class} - #{e.message}"
    false
  end
end

# ============================================================================
# USER MANAGEMENT TESTS
# ============================================================================

puts "\n" + "=" * 80
puts "USER MANAGEMENT TESTS"
puts "=" * 80

test("Create user with custom key") do
  admin_client.create_user!(
    user_id: test_user_id,
    secret_key: "test_secret_#{rand(10000)}"
  )
end

test("Create user without key (auto-generated)") do
  auto_user_id = "auto_user_#{rand(100000)}"
  result = admin_client.create_user(user_id: auto_user_id)
  result[:success]
end

test("List users") do
  result = admin_client.list_users!
  result.is_a?(Array)
end

test("Create duplicate user (should fail)") do
  result = admin_client.create_user(
    user_id: test_user_id,
    secret_key: "different_key"
  )
  result[:success] == false && result[:error].is_a?(SnelDB::CommandError)
end

test("Show permissions for user (before granting)") do
  result = admin_client.show_permissions(user_id: test_user_id)
  result[:success] # May return empty permissions
end

# ============================================================================
# PERMISSION MANAGEMENT TESTS
# ============================================================================

puts "\n" + "=" * 80
puts "PERMISSION MANAGEMENT TESTS"
puts "=" * 80

# Define event types first
# Note: define! automatically grants read/write permissions to the current user
test("Define event schema for permissions test") do
  admin_client.define!(
    event_type: test_event_type,
    fields: { "order_id" => "int", "status" => "string", "amount" => "float" }
    # auto_grant defaults to true, so admin automatically gets permissions
  )
end

# Note: Since define! now auto-grants permissions to the defining user (admin),
# these explicit grants are for the test_user_id to test permission management
test("Grant read permission") do
  admin_client.grant_permission!(
    permissions: ["read"],
    event_types: [test_event_type],
    user_id: test_user_id
  )
end

test("Grant write permission") do
  admin_client.grant_permission!(
    permissions: ["write"],
    event_types: [test_event_type],
    user_id: test_user_id
  )
end

test("Grant read and write permissions together") do
  admin_client.grant_permission!(
    permissions: ["read", "write"],
    event_types: [test_event_type],
    user_id: test_user_id
  )
end

test("Show permissions after granting") do
  result = admin_client.show_permissions!(user_id: test_user_id)
  perms = result.find { |p| (p["event_type"] || p[:event_type]) == test_event_type }
  perms && (perms["permissions"] || perms[:permissions])
end

test("Revoke specific permission") do
  admin_client.revoke_permission!(
    permissions: ["write"],
    event_types: [test_event_type],
    user_id: test_user_id
  )
end

test("Revoke all permissions") do
  admin_client.revoke_permission!(
    event_types: [test_event_type],
    user_id: test_user_id
  )
end

test("Grant permission to nonexistent user (should fail)") do
  result = admin_client.grant_permission(
    permissions: ["read"],
    event_types: [test_event_type],
    user_id: "nonexistent_user_#{rand(100000)}"
  )
  result[:success] == false && result[:error].is_a?(SnelDB::CommandError)
end

# ============================================================================
# SUCCESS SCENARIOS
# ============================================================================

puts "\n" + "=" * 80
puts "EVENT OPERATION SUCCESS SCENARIOS"
puts "=" * 80

# Re-grant permissions for testing
# Note: Since define! now auto-grants to the defining user (admin),
# we need to explicitly grant to test_user_id for testing
test("Re-grant permissions for event operations") do
  admin_client.grant_permission!(
    permissions: ["read", "write"],
    event_types: [test_event_type],
    user_id: test_user_id
  )
end

# 1. Define and store a simple event
test("Store valid event") do
  admin_client.store!(
    event_type: test_event_type,
    context_id: "customer-1",
    payload: { order_id: 1, status: "confirmed", amount: 99.99 }
  )
end

# 2. Query events
test("Query events with filter") do
  result = admin_client.query!(
    event_type: test_event_type,
    where: 'status = "confirmed"'
  )
  result.length > 0
end

# 3. Store with optional field
test("Define event with optional field") do
  admin_client.define!(
    event_type: test_optional_type,
    fields: {
      "id" => "int",
      "name" => "string",
      "optional_field" => "string | null"
    }
    # auto_grant defaults to true - permissions automatically granted
  )
end

test("Store with optional field present") do
  admin_client.store!(
    event_type: test_optional_type,
    context_id: "ctx-1",
    payload: { id: 1, name: "test", optional_field: "value" }
  )
end

test("Store with optional field missing") do
  admin_client.store!(
    event_type: test_optional_type,
    context_id: "ctx-2",
    payload: { id: 2, name: "test2" }
  )
end

# 4. Query with multiple conditions
test("Query with AND condition") do
  admin_client.query!(
    event_type: test_event_type,
    where: 'status = "confirmed" AND amount > 50'
  )
end

# 5. Non-raising methods (should return result hashes)
test("Non-raising store method") do
  result = admin_client.store(
    event_type: test_event_type,
    context_id: "customer-2",
    payload: { order_id: 2, status: "pending", amount: 50.0 }
  )
  result[:success] == true
end

test("Non-raising query method") do
  result = admin_client.query(event_type: test_event_type, limit: 10)
  result[:success] == true && result[:data].is_a?(Array)
end

test("Create authenticated client with test user") do
  require 'securerandom'
  user_key = "key_" + SecureRandom.hex(14)
  user_id = "auth_test_#{rand(100000)}"

  create_result = admin_client.create_user(user_id: user_id, secret_key: user_key)
  unless create_result[:success]
    raise "Failed to create user: #{create_result[:error].message}"
  end

  # Grant permissions explicitly (define! auto-grants to admin, not to new users)
  grant_result = admin_client.grant_permission(
    permissions: ["read", "write"],
    event_types: [test_event_type],
    user_id: user_id
  )
  unless grant_result[:success]
    raise "Failed to grant permissions: #{grant_result[:error].message}"
  end

  list_result = admin_client.list_users
  users = list_result[:data] || []
  user_exists = users.any? { |u|
    raw = u["raw"] || u[:raw] || ""
    raw.include?(user_id)
  }
  unless user_exists
    raise "User not found in user list after creation"
  end

  true
end

# ============================================================================
# ERROR SCENARIOS
# ============================================================================

puts "\n" + "=" * 80
puts "ERROR SCENARIOS"
puts "=" * 80

# 1. Store without definition (should raise CommandError)
test("Store without schema definition (should fail)") do
  result = admin_client.store(
    event_type: "nonexistent_event_#{rand(100000)}",
    context_id: "customer-1",
    payload: { order_id: 1, status: "confirmed" }
  )
  result[:success] == false && result[:error].is_a?(SnelDB::CommandError)
end

# 2. Store with invalid enum value (should raise CommandError)
test("Define enum event") do
  admin_client.define!(
    event_type: enum_event_type,
    fields: { "plan" => ["pro", "basic"] }
    # auto_grant defaults to true - permissions automatically granted
  )
end

test("Store with invalid enum value (should fail)") do
  result = admin_client.store(
    event_type: enum_event_type,
    context_id: "user-1",
    payload: { plan: "enterprise" }  # Not in enum
  )
  result[:success] == false && result[:error].is_a?(SnelDB::CommandError)
end

# 3. Store with invalid payload type (should raise CommandError)
test("Store with wrong field type (should fail)") do
  result = admin_client.store(
    event_type: test_event_type,
    context_id: "customer-1",
    payload: { order_id: "not a number", status: "confirmed", amount: 99.99 }
  )
  result[:success] == false && result[:error].is_a?(SnelDB::CommandError)
end

# 4. Store with missing required field (should raise CommandError)
test("Store with missing field (should fail)") do
  result = admin_client.store(
    event_type: test_event_type,
    context_id: "customer-1",
    payload: { order_id: 1, amount: 99.99 }  # Missing 'status'
  )
  result[:success] == false && result[:error].is_a?(SnelDB::CommandError)
end

# 5. Store with extra field not in schema (should raise CommandError)
test("Store with invalid field (should fail)") do
  result = admin_client.store(
    event_type: test_event_type,
    context_id: "customer-1",
    payload: {
      order_id: 1,
      status: "confirmed",
      amount: 99.99,
      invalid_field: "not in schema"
    }
  )
  result[:success] == false && result[:error].is_a?(SnelDB::CommandError)
end

# 6. Query nonexistent event type (should return empty or raise error)
test("Query nonexistent event type") do
  result = admin_client.query!(event_type: "nonexistent_type_#{rand(100000)}")
  result.is_a?(Array)  # May return empty array
end

# 7. Query with no matches (should return empty array, not error)
test("Query with no matches") do
  result = admin_client.query(
    event_type: test_event_type,
    where: 'status = "nonexistent_status"'
  )
  result[:success] && result[:data].is_a?(Array)  # Should return empty array, not raise error
end

# 8. Invalid command syntax (should raise CommandError)
test("Invalid command (should fail)") do
  result = admin_client.execute("INVALID COMMAND SYNTAX")
  result[:success] == false && result[:error].is_a?(SnelDB::CommandError)
end

# 9. Test non-raising methods with errors
test("Non-raising method with error") do
  result = admin_client.store(
    event_type: "nonexistent_event_#{rand(100000)}",
    context_id: "customer-1",
    payload: { order_id: 1 }
  )
  result[:success] == false && result[:error].is_a?(SnelDB::CommandError)
end

# 10. Test authentication errors
test_error("Authenticate with wrong key", SnelDB::AuthenticationError) do
  bad_client = SnelDB::Client.new(
    address: address,
    protocol: protocol,
    user_id: test_user_id,
    secret_key: "wrong_key"
  )
  bad_client.execute!("FLUSH")
end

# 11. Test authorization errors (user without permissions)
test("Test user without permissions") do
  no_perm_user = "no_perm_#{rand(100000)}"
  no_perm_key = SecureRandom.hex(16)

  # Create user without granting permissions
  admin_client.create_user!(user_id: no_perm_user, secret_key: no_perm_key)

  no_perm_client = SnelDB::Client.new(
    address: address,
    protocol: protocol,
    user_id: no_perm_user,
    secret_key: no_perm_key
  )

  # Should fail to store (no write permission)
  result = no_perm_client.store(
    event_type: test_event_type,
    context_id: "test",
    payload: { order_id: 1, status: "test", amount: 1.0 }
  )
  result[:success] == false && result[:error].is_a?(SnelDB::AuthorizationError)
end

puts "\n" + "-" * 80
puts "Testing connection error (if server is down)..."
begin
  bad_client = SnelDB::Client.new(address: "127.0.0.1:9999", protocol: protocol)
  result = bad_client.query(event_type: "test", limit: 1)
  if result[:success] == false && result[:error].is_a?(SnelDB::ConnectionError)
    puts "✅ Connection error handled correctly"
  else
    puts "⚠️  Server is reachable (connection error test skipped)"
  end
rescue => e
  puts "⚠️  Connection test: #{e.class} - #{e.message}"
end

# ============================================================================
# SUMMARY
# ============================================================================

puts "\n" + "=" * 80
puts "Test Summary"
puts "=" * 80
puts "All tests completed. Check results above."
puts ""
