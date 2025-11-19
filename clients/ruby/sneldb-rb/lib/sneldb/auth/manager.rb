require "openssl"
require_relative "../errors"

module SnelDB
  module Auth
    # Manages authentication for SnelDB client
    class Manager
      attr_reader :user_id, :secret_key, :session_token, :authenticated_user_id

      def initialize(user_id: nil, secret_key: nil)
        @user_id = user_id
        @secret_key = secret_key
        @session_token = nil
        @authenticated_user_id = nil
      end

      # Compute HMAC-SHA256 signature for a message
      # @param message [String] The message to sign
      # @return [String] Hex-encoded signature
      def compute_signature(message)
        raise AuthenticationError, "Secret key not set" unless @secret_key

        digest = OpenSSL::Digest.new("sha256")
        trimmed_message = message.strip
        OpenSSL::HMAC.hexdigest(digest, @secret_key, trimmed_message)
      end

      # Authenticate using AUTH command (for TCP/WebSocket)
      # @param transport [Transport::TCP] The TCP transport
      # @return [String] Session token
      def authenticate!(transport)
        raise AuthenticationError, "User ID and secret key required for authentication" unless @user_id && @secret_key
        raise AuthenticationError, "AUTH command requires TCP transport" unless transport.is_a?(Transport::TCP)

        # Ensure transport is connected
        transport.connect

        # Compute signature for AUTH command
        # Signature is computed as: HMAC-SHA256(secret_key, user_id)
        signature = compute_signature(@user_id)

        command = "AUTH #{@user_id}:#{signature}"
        response = transport.execute(command)

        if response[:status] == 200 && response[:body].start_with?("OK TOKEN")
          # Extract token from "OK TOKEN <token>"
          token_match = response[:body].match(/OK TOKEN\s+(\S+)/)
          if token_match
            @session_token = token_match[1]
            @authenticated_user_id = @user_id
            transport.session_token = @session_token
            transport.authenticated_user_id = @authenticated_user_id
            return @session_token
          end
        end

        raise AuthenticationError, "Authentication failed: #{response[:body]}"
      end

      # Add authentication headers for HTTP requests
      # @param command [String] The command to authenticate
      # @param headers [Hash] Existing headers hash
      # @return [Hash] Headers with authentication added
      def add_http_headers(command, headers: {})
        return headers unless @user_id && @secret_key

        signature = compute_signature(command)
        headers["X-Auth-User"] = @user_id
        headers["X-Auth-Signature"] = signature
        headers
      end

      # Format command with authentication for TCP
      # Supports multiple formats:
      # 1. Session token: "COMMAND ... TOKEN <token>"
      # 2. Connection-scoped: "signature:COMMAND ..." (after AUTH)
      # 3. Inline: "user_id:signature:COMMAND ..."
      # @param command [String] The command to format
      # @param transport [Transport::TCP] The TCP transport
      # @return [String] Formatted command with authentication
      def format_tcp_command(command, transport)
        # If we have a session token, use it
        if @session_token || transport.session_token
          token = @session_token || transport.session_token
          return "#{command.strip} TOKEN #{token}"
        end

        # If connection is authenticated, use connection-scoped format
        if @authenticated_user_id || transport.authenticated_user_id
          signature = compute_signature(command)
          return "#{signature}:#{command.strip}"
        end

        # Otherwise, use inline format
        if @user_id && @secret_key
          signature = compute_signature(command)
          return "#{@user_id}:#{signature}:#{command.strip}"
        end

        # No authentication
        command
      end

      # Set session token (after successful AUTH)
      def session_token=(token)
        @session_token = token
      end

      # Set authenticated user ID (after successful AUTH)
      def authenticated_user_id=(user_id)
        @authenticated_user_id = user_id
      end

      # Clear authentication state
      def clear
        @session_token = nil
        @authenticated_user_id = nil
      end
    end
  end
end

