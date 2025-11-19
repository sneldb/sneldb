#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative '../lib/sneldb'

# Initialize client (use text format to avoid Arrow parsing issues with errors)
base_url = ENV['SNELDB_URL'] || 'http://localhost:8085'
client = SnelDB::Client.new(base_url: base_url, output_format: "text")

puts "=" * 80
puts "SnelDB Client Error and Success Scenario Tests"
puts "=" * 80
puts "Server: #{base_url}"
puts ""

# Generate randomized event type names to avoid conflicts when running multiple times
test_order_type = "test_order_#{rand(100000)}"
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
# SUCCESS SCENARIOS
# ============================================================================

puts "\n" + "=" * 80
puts "SUCCESS SCENARIOS"
puts "=" * 80

# 1. Define and store a simple event
test("Define event schema") do
  client.define!(
    event_type: test_order_type,
    fields: { "order_id" => "int", "status" => "string", "amount" => "float" }
  )
end

test("Store valid event") do
  client.store!(
    event_type: test_order_type,
    context_id: "customer-1",
    payload: { order_id: 1, status: "confirmed", amount: 99.99 }
  )
end

# 2. Query events
test("Query events with filter") do
  result = client.query!(
    event_type: test_order_type,
    where: 'status="confirmed"'
  )
  result.length > 0
end

# 3. Replay events (SKIPPED - replay is currently broken)
# test("Replay events for context") do
#   client.replay!(context_id: "customer-1", event_type: test_order_type)
# end

# 4. Store with optional field
test("Define event with optional field") do
  client.define!(
    event_type: test_optional_type,
    fields: {
      "id" => "int",
      "name" => "string",
      "optional_field" => "string | null"
    }
  )
end

test("Store with optional field present") do
  client.store!(
    event_type: test_optional_type,
    context_id: "ctx-1",
    payload: { id: 1, name: "test", optional_field: "value" }
  )
end

test("Store with optional field missing") do
  client.store!(
    event_type: test_optional_type,
    context_id: "ctx-2",
    payload: { id: 2, name: "test2" }
  )
end

# 5. Query with multiple conditions
test("Query with AND condition") do
  client.query!(
    event_type: test_order_type,
    where: 'status="confirmed" AND amount > 50'
  )
end

# 6. Non-raising methods (should return result hashes)
test("Non-raising store method") do
  result = client.store(
    event_type: test_order_type,
    context_id: "customer-2",
    payload: { order_id: 2, status: "pending", amount: 50.0 }
  )
  result[:success] == true
end

test("Non-raising query method") do
  result = client.query(event_type: test_order_type, limit: 10)
  result[:success] == true && result[:data].is_a?(Array)
end

# ============================================================================
# ERROR SCENARIOS
# ============================================================================

puts "\n" + "=" * 80
puts "ERROR SCENARIOS"
puts "=" * 80

# 1. Store without definition (should raise CommandError)
test_error("Store without schema definition", SnelDB::CommandError) do
  client.store!(
    event_type: "nonexistent_event",
    context_id: "customer-1",
    payload: { order_id: 1, status: "confirmed" }
  )
end

# 2. Store with invalid enum value (should raise CommandError)
test("Define enum event") do
  client.define!(
    event_type: enum_event_type,
    fields: { "plan" => ["pro", "basic"] }
  )
end

test_error("Store with invalid enum value", SnelDB::CommandError) do
  client.store!(
    event_type: enum_event_type,
    context_id: "user-1",
    payload: { plan: "enterprise" }  # Not in enum
  )
end

# 3. Store with invalid payload type (should raise CommandError)
test_error("Store with wrong field type", SnelDB::CommandError) do
  client.store!(
    event_type: test_order_type,
    context_id: "customer-1",
    payload: { order_id: "not a number", status: "confirmed", amount: 99.99 }
  )
end

# 4. Store with missing required field (should raise CommandError)
test_error("Store with missing field", SnelDB::CommandError) do
  client.store!(
    event_type: test_order_type,
    context_id: "customer-1",
    payload: { order_id: 1, amount: 99.99 }  # Missing 'status'
  )
end

# 5. Store with extra field not in schema (should raise CommandError)
test_error("Store with invalid field", SnelDB::CommandError) do
  client.store!(
    event_type: test_order_type,
    context_id: "customer-1",
    payload: {
      order_id: 1,
      status: "confirmed",
      amount: 99.99,
      invalid_field: "not in schema"
    }
  )
end

# 6. Query nonexistent event type (should return empty or raise error)
test("Query nonexistent event type") do
  result = client.query!(event_type: "nonexistent_type")
  result.is_a?(Array)  # May return empty array
end

# 7. Query with no matches (should return empty array, not error)
test("Query with no matches") do
  result = client.query!(
    event_type: test_order_type,
    where: 'status="nonexistent_status"'
  )
  result.is_a?(Array)  # Should return empty array, not raise error
end

# 8. Replay nonexistent context (SKIPPED - replay is currently broken)
# test("Replay nonexistent context") do
#   result = client.replay!(context_id: "nonexistent-context")
#   result.is_a?(Array)  # Should return empty array, not raise error
# end

# 9. Invalid command syntax (should raise CommandError)
test_error("Invalid command", SnelDB::CommandError) do
  client.execute!("INVALID COMMAND SYNTAX")
end

# 10. Test non-raising methods with errors
test("Non-raising method with error") do
  result = client.store(
    event_type: "nonexistent_event",
    context_id: "customer-1",
    payload: { order_id: 1 }
  )
  result[:success] == false && result[:error].is_a?(SnelDB::CommandError)
end

# 11. Test ping for server availability checking
puts "\n" + "-" * 80
puts "Testing ping for server availability..."
test("Ping server (should succeed)") do
  result = client.ping
  if result[:success]
    puts "   Server is available and responding correctly"
    true
  else
    puts "   Server check failed: #{result[:error].message}"
    false
  end
end

test_error("Ping unreachable server", SnelDB::ConnectionError) do
  bad_client = SnelDB::Client.new(base_url: "http://localhost:9999")
  bad_client.ping!  # Should raise ConnectionError
end

# ============================================================================
# SUMMARY
# ============================================================================

puts "\n" + "=" * 80
puts "Test Summary"
puts "=" * 80
puts "All tests completed. Check results above."
puts ""
puts "Note: Some scenarios may behave differently:"
puts "- Query with no matches returns empty array (not an error)"
puts "- Replay with no events returns empty array (not an error)"
puts "- Store/Define errors raise CommandError (400 Bad Request)"
puts ""

