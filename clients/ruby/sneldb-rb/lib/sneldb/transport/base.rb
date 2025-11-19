module SnelDB
  module Transport
    # Base transport interface
    class Base
      attr_reader :host, :port, :use_ssl

      def initialize(host:, port:, use_ssl: false)
        @host = host
        @port = port
        @use_ssl = use_ssl
      end

      # Execute a command and return the raw response
      # @param command [String] The command to execute
      # @param headers [Hash] Optional headers
      # @return [Hash] Response with :status, :body, :headers
      def execute(command, headers: {})
        raise NotImplementedError, "Subclasses must implement #execute"
      end

      # Close the connection (if applicable)
      def close
        # Default: no-op for stateless transports
      end
    end
  end
end

