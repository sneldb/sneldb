#!/usr/bin/env ruby

# Example Rails model with SnelDB integration

class Order < ApplicationRecord
  belongs_to :customer

  # Define events that will be stored automatically after commits
  sneldb_event :order_created, on: :create do
    {
      id: id,
      customer_id: customer_id,
      amount: total_amount,
      currency: currency,
      status: status,
      created_at: created_at.iso8601
    }
  end

  sneldb_event :order_updated, on: :update do
    {
      id: id,
      customer_id: customer_id,
      amount: total_amount,
      status: status,
      updated_at: updated_at.iso8601,
      changes: saved_changes.except("updated_at")
    }
  end

  sneldb_event :order_cancelled, on: :destroy do
    {
      id: id,
      customer_id: customer_id,
      cancelled_at: Time.current.iso8601
    }
  end

  # Customize context ID to use customer ID
  def sneldb_context_id
    "customer-#{customer_id}"
  end
end

# Usage:
# order = Order.create!(customer_id: 123, total_amount: 99.99, currency: "USD")
# # => Automatically stores "order_created" event after commit

# order.update!(status: "shipped")
# # => Automatically stores "order_updated" event after commit

# order.destroy
# # => Automatically stores "order_cancelled" event after commit

