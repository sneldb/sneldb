require_relative "client"
require_relative "errors"

module SnelDB
  module Rails
    class << self
      attr_accessor :client

      # Configure the SnelDB client for Rails
      def configure(address:, protocol: nil, user_id: nil, secret_key: nil, output_format: "text")
        @client = Client.new(
          address: address,
          protocol: protocol,
          user_id: user_id,
          secret_key: secret_key,
          output_format: output_format
        )
      end

      # Get or initialize the client
      def client
        @client ||= begin
          if defined?(::Rails) && ::Rails.application.config.sneldb
            config = ::Rails.application.config.sneldb
            Client.new(
              address: config[:address] || "http://localhost:8085",
              protocol: config[:protocol],
              user_id: config[:user_id],
              secret_key: config[:secret_key],
              output_format: config[:output_format] || "text"
            )
          else
            raise ConfigurationError, "SnelDB client not configured. Set up config/initializers/sneldb.rb"
          end
        end
      end

      # Define an event (stores definition for later registration)
      def define_event(event_type:, fields:, version: nil)
        @event_definitions ||= []
        @event_definitions << {
          event_type: event_type.to_s,
          fields: fields,
          version: version
        }
      end

      # Register all defined events with SnelDB
      def define_events
        return unless @event_definitions

        @event_definitions.each do |defn|
          begin
            client.define!(
              event_type: defn[:event_type],
              fields: defn[:fields],
              version: defn[:version],
              auto_grant: true  # Automatically grant permissions when defining
            )
            puts "✓ Defined event: #{defn[:event_type]}"
          rescue Error => e
            puts "✗ Failed to define event #{defn[:event_type]}: #{e.message}"
          end
        end
      end

      # Store an event
      def store_event(event_type:, context_id:, payload:)
        client.store!(
          event_type: event_type.to_s,
          context_id: context_id.to_s,
          payload: payload
        )
      end
    end

    class ConfigurationError < Error; end
  end
end

