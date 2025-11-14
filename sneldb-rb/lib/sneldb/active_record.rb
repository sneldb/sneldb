require "active_support/concern"
require_relative "rails"

module SnelDB
  module ActiveRecord
    extend ActiveSupport::Concern

    included do
      # Store events after successful commits
      after_commit :store_sneldb_events, on: [:create, :update, :destroy]
    end

    class_methods do
      # Define an event to be stored when this model is created/updated/destroyed
      #
      # @param event_type [String, Symbol] The event type name
      # @param on [Symbol, Array<Symbol>] When to store: :create, :update, :destroy (default: :create)
      # @param block [Proc] Block that returns the event payload hash
      #
      # Example:
      #   sneldb_event :order_created, on: :create do
      #     {
      #       id: id,
      #       amount: total_amount,
      #       currency: currency,
      #       created_at: created_at.iso8601
      #     }
      #   end
      def sneldb_event(event_type, on: :create, &block)
        event_type = event_type.to_s
        on = Array(on)

        # Store event configuration
        sneldb_events_config[event_type] = {
          on: on,
          payload_block: block
        }
      end

      # Get all configured events for this model
      def sneldb_events_config
        @sneldb_events_config ||= {}
      end
    end

    private

    # Store events after commit
    def store_sneldb_events
      return unless self.class.sneldb_events_config.any?

      action = if destroyed?
        :destroy
      elsif saved_change_to_id?
        :create
      else
        :update
      end

      self.class.sneldb_events_config.each do |event_type, config|
        next unless config[:on].include?(action)

        begin
          payload = instance_eval(&config[:payload_block])
          # Skip if payload is nil (condition not met)
          next if payload.nil?

          context_id = sneldb_context_id

          SnelDB::Rails.store_event(
            event_type: event_type,
            context_id: context_id,
            payload: payload
          )
        rescue StandardError => e
          # Log error but don't fail the transaction
          if defined?(Rails) && Rails.respond_to?(:logger)
            Rails.logger.error("Failed to store SnelDB event #{event_type}: #{e.message}")
          end
        end
      end
    end

    # Get the context ID for this record
    # Override this method in your model to customize context ID generation
    def sneldb_context_id
      # Helper to check if value is present
      present = ->(val) { val.respond_to?(:present?) ? val.present? : !val.nil? && val != "" }

      # Try common context ID patterns
      if respond_to?(:user_id) && (user_id_val = user_id) && present.call(user_id_val)
        "user-#{user_id_val}"
      elsif respond_to?(:customer_id) && (customer_id_val = customer_id) && present.call(customer_id_val)
        "customer-#{customer_id_val}"
      elsif respond_to?(:account_id) && (account_id_val = account_id) && present.call(account_id_val)
        "account-#{account_id_val}"
      elsif respond_to?(:id) && (id_val = id) && present.call(id_val)
        # Fallback to model name and ID
        "#{self.class.name.underscore}-#{id_val}"
      else
        # Last resort: use class name
        self.class.name.underscore
      end
    end
  end
end

# Auto-include in ActiveRecord::Base if available
if defined?(::ActiveRecord::Base)
  ::ActiveRecord::Base.include SnelDB::ActiveRecord
end

