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

          # Check if this is a streaming JSON end frame (for queries/replay)
          # This can appear at any point, not just after status lines
          if line.match?(/"type"\s*:\s*"end"/)
            break
          end

          # Check if this is an error response
          # Error responses can be:
          # - "ERROR: message"
          # - "400 message" (HTTP-style error status)
          # - "401 message", "403 message", "404 message", "500 message", etc.
          if line.start_with?("ERROR:") || line.match?(/^(400|401|403|404|500)\s+/)
            break
          end

          # Check if this is a success response (OK or 200 OK)
          if line.start_with?("OK") || line.match?(/^\d{3}\s+OK/)
            # For AUTH command, we get "OK TOKEN <token>" on single line
            if line.include?("TOKEN")
              break
            end
            # For "200 OK" or "OK", continue reading to get the response body
            # Read all content lines until empty line, EOF, or streaming end frame
            loop do
              # Use non-blocking check to see if there's more data
              if IO.select([@socket], nil, nil, 0.1)  # 100ms timeout
                content_line = @socket.gets
                if content_line.nil?
                  # EOF, response complete
                  break
                end
                content_line = content_line.chomp

                if content_line.empty?
                  # Empty line indicates end of multi-line response
                  break
                else
                  # More content, add it
                  response_lines << content_line

                  # Check if this is a streaming JSON end frame (for queries/replay)
                  # Streaming format: {"type":"end","row_count":N}
                  if content_line.match?(/"type"\s*:\s*"end"/)
                    break
                  end
                end
              else
                # No more data available, response complete
                break
              end

              # Safety limit
              if response_lines.length > 1000
                break
              end
            end
            # Done reading content lines, exit the main loop
            break
          end

          # Limit to prevent infinite loops
          if response_lines.length > 1000
            break
          end
        end

        body = response_lines.join("\n")

        # Parse status from response
        # Check first line for status code
        first_line = response_lines.first || ""
        status = if first_line.start_with?("ERROR:")
          400
        elsif first_line.match?(/^(400|401|403|404|500)\s+/)
          # Extract status code from HTTP-style error (e.g., "400 User already exists")
          first_line.match(/^(\d{3})/)[1].to_i rescue 400
        elsif first_line.start_with?("OK") || first_line.match?(/^200\s+OK/)
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

