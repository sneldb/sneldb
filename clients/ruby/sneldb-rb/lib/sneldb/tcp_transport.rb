require "socket"
require "openssl"
require_relative "errors"

module SnelDB
  # TCP transport for SnelDB client
  # Handles line-based TCP protocol communication
  class TcpTransport
    attr_reader :host, :port, :user_id, :secret_key, :session_token

    # Initialize TCP transport
    #
    # @param host [String] Server hostname or IP
    # @param port [Integer] Server port
    # @param user_id [String, nil] Optional user ID for authentication
    # @param secret_key [String, nil] Optional secret key for authentication
    def initialize(host:, port:, user_id: nil, secret_key: nil)
      @host = host
      @port = port
      @user_id = user_id
      @secret_key = secret_key
      @session_token = nil
      @socket = nil
      @read_timeout = 60
    end

    # Connect to the server and authenticate if credentials are provided
    #
    # @raise [SnelDB::ConnectionError] if connection fails
    # @raise [SnelDB::AuthenticationError] if authentication fails
    def connect
      begin
        @socket = TCPSocket.new(@host, @port)
        @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
      rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH, Errno::ETIMEDOUT => e
        raise ConnectionError, "Cannot connect to server at #{@host}:#{@port}: #{e.message}"
      rescue SocketError => e
        raise ConnectionError, "Network error: #{e.message}"
      rescue => e
        raise ConnectionError, "Connection error: #{e.class} - #{e.message}"
      end

      # Authenticate if credentials are provided
      if @user_id && @secret_key
        authenticate
      end
    end

    # Execute a command and return the response
    #
    # @param command [String] The command to execute
    # @return [String] The response body
    # @raise [SnelDB::ConnectionError] if connection errors occur
    # @raise [SnelDB::AuthenticationError] if authentication fails
    # @raise [SnelDB::CommandError] if command is invalid
    # @raise [SnelDB::Error] for other errors
    def execute(command)
      ensure_connected

      # Format command with authentication
      authenticated_cmd = format_command(command)

      begin
        # Send command
        @socket.write(authenticated_cmd)
        @socket.flush

        # Read response (line-based)
        response = read_response

        # Check for errors in response
        if response.start_with?("ERROR:")
          error_msg = response.sub(/^ERROR:\s*/, "").strip

          # Try to determine error type from message
          if error_msg.downcase.include?("authentication") || error_msg.downcase.include?("auth")
            raise AuthenticationError, error_msg
          elsif error_msg.downcase.include?("not found")
            raise NotFoundError, error_msg
          elsif error_msg.downcase.include?("shutting down") || error_msg.downcase.include?("under pressure")
            raise ConnectionError, error_msg
          else
            raise CommandError, error_msg
          end
        end

        # Strip trailing newline if present
        response.strip
      rescue Errno::EPIPE, Errno::ECONNRESET => e
        # Connection lost, reset state
        @socket = nil
        @session_token = nil
        raise ConnectionError, "Connection lost: #{e.message}"
      rescue IOError, SystemCallError => e
        raise ConnectionError, "I/O error: #{e.message}"
      end
    end

    # Close the connection
    def close
      if @socket && !@socket.closed?
        @socket.close
      end
      @socket = nil
      @session_token = nil
    end

    # Check if connection is open
    def connected?
      @socket && !@socket.closed?
    end

    private

    # Ensure connection is established
    def ensure_connected
      unless connected?
        connect
      end
    end

    # Authenticate the connection using AUTH command
    def authenticate
      # Compute signature: HMAC-SHA256(secret_key, user_id)
      signature = compute_hmac(@secret_key, @user_id)
      auth_cmd = "AUTH #{@user_id}:#{signature}\n"

      begin
        @socket.write(auth_cmd)
        @socket.flush

        response = read_response.strip

        if response.start_with?("OK TOKEN ")
          # Extract session token for fast path
          @session_token = response.sub(/^OK TOKEN\s+/, "").strip
        elsif response == "OK"
          # Authentication succeeded but no token
          @session_token = nil
        else
          raise AuthenticationError, "Authentication failed: #{response}"
        end
      rescue => e
        if e.is_a?(AuthenticationError)
          raise
        end
        raise AuthenticationError, "Authentication error: #{e.message}"
      end
    end

    # Format command with authentication
    def format_command(command)
      cmd_trimmed = command.strip

      if @session_token
        # Fast path: use session token
        "#{cmd_trimmed} TOKEN #{@session_token}\n"
      elsif @user_id && @secret_key
        # Use HMAC signature per command
        signature = compute_hmac(@secret_key, cmd_trimmed)
        "#{signature}:#{cmd_trimmed}\n"
      else
        # No authentication
        "#{cmd_trimmed}\n"
      end
    end

    # Read response from socket (line-based)
    # TCP protocol uses line-based responses (commands end with \n, responses end with \n)
    def read_response
      line = @socket.gets
      if line.nil?
        raise ConnectionError, "Connection closed by server"
      end

      # Remove trailing newline
      line.chomp
    rescue Errno::ETIMEDOUT, Timeout::Error => e
      raise ConnectionError, "Read timeout: #{e.message}"
    rescue IOError, SystemCallError => e
      raise ConnectionError, "Read error: #{e.message}"
    end

    # Compute HMAC-SHA256 signature
    def compute_hmac(key, message)
      digest = OpenSSL::Digest.new("sha256")
      OpenSSL::HMAC.hexdigest(digest, key, message.strip)
    end
  end
end

