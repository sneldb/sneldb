require "socket"
require "openssl"
require_relative "base"

module SnelDB
  module Transport
    # TCP transport implementation
    class TCP < Base
      attr_reader :socket
      attr_accessor :authenticated_user_id, :session_token

      def initialize(host:, port:, use_ssl: false, read_timeout: 60)
        super(host: host, port: port, use_ssl: use_ssl)
        @read_timeout = read_timeout
        @socket = nil
        @authenticated_user_id = nil
        @session_token = nil
        @buffer = ""
      end

      def connect
        return if @socket && !@socket.closed?

        @socket = TCPSocket.new(@host, @port)
        @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

        if @use_ssl
          ssl_context = OpenSSL::SSL::SSLContext.new
          @socket = OpenSSL::SSL::SSLSocket.new(@socket, ssl_context)
          @socket.sync_close = true
          @socket.connect
        end
      rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH, Errno::ETIMEDOUT => e
        raise ConnectionError, "Cannot connect to server at #{@host}:#{@port}: #{e.message}"
      rescue SocketError => e
        raise ConnectionError, "Network error: #{e.message}"
      rescue OpenSSL::SSL::SSLError => e
        raise ConnectionError, "SSL error: #{e.message}"
      rescue => e
        raise ConnectionError, "Connection error: #{e.class} - #{e.message}"
      end

      def execute(command, headers: {})
        connect if @socket.nil? || @socket.closed?

        # Send command with newline
        @socket.puts(command)
        @socket.flush

        # Read response (lines until we get a complete response)
        response_lines = []
        loop do
          line = read_line_with_timeout
          break if line.nil?

          line = line.chomp
          response_lines << line

          # Check if this is an error response
          if line.start_with?("ERROR:")
            break
          end

          # Check if this is a success response (OK or 200 OK)
          if line.start_with?("OK") || line.match?(/^\d{3}\s+OK/)
            # For AUTH command, we get "OK TOKEN <token>" on single line
            # For other commands, we might get multi-line responses
            # Check if next line is empty or if we've read enough
            break if line.strip == "OK" || line.strip.match?(/^\d{3}\s+OK$/) || line.include?("TOKEN")
          end

          # For multi-line responses, read until empty line
          # But limit to prevent infinite loops
          break if response_lines.length > 1000
        end

        body = response_lines.join("\n")

        # Parse status from response
        status = if body.start_with?("ERROR:")
          400
        elsif body.start_with?("OK")
          200
        else
          200 # Default to success for TCP
        end

        {
          status: status,
          body: body,
          headers: {}
        }
      rescue Errno::ECONNRESET, Errno::EPIPE => e
        @socket = nil
        raise ConnectionError, "Connection lost: #{e.message}"
      rescue => e
        @socket = nil
        raise ConnectionError, "TCP error: #{e.class} - #{e.message}"
      end

      def close
        return if @socket.nil? || @socket.closed?

        @socket.close
        @socket = nil
        @authenticated_user_id = nil
        @session_token = nil
      end

      private

      def read_line_with_timeout
        return nil if @socket.nil? || @socket.closed?

        # Simple timeout using select
        if IO.select([@socket], nil, nil, @read_timeout)
          @socket.gets
        else
          raise ConnectionError, "Read timeout"
        end
      end
    end
  end
end

