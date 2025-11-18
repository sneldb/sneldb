require "net/http"
require "uri"
require "json"
require "openssl"
require_relative "errors"

module SnelDB
  # HTTP transport for SnelDB client
  # Handles HTTP-based communication with the server
  class HttpTransport
    attr_reader :base_url, :user_id, :secret_key

    # Initialize HTTP transport
    #
    # @param base_url [String] The base URL of the SnelDB server (e.g., "http://localhost:8085")
    # @param user_id [String, nil] Optional user ID for authentication
    # @param secret_key [String, nil] Optional secret key for authentication
    def initialize(base_url:, user_id: nil, secret_key: nil)
      @base_url = base_url.chomp("/")
      @user_id = user_id
      @secret_key = secret_key
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
      begin
        uri = URI("#{@base_url}/command")
      rescue URI::InvalidURIError => e
        raise ConnectionError, "Invalid server URL: #{@base_url} - #{e.message}"
      end

      begin
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = uri.scheme == "https"
        http.read_timeout = 60

        request = Net::HTTP::Post.new(uri.path)
        request.body = command
        request["Content-Type"] = "text/plain"

        # Add authentication headers if credentials are provided
        if @user_id && @secret_key
          # Compute signature on trimmed command (server trims before verifying)
          signature = compute_signature(command)
          request["X-Auth-User"] = @user_id
          request["X-Auth-Signature"] = signature
        end

        response = http.request(request)
      rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH, Errno::ETIMEDOUT => e
        raise ConnectionError, "Cannot connect to server at #{@base_url}: #{e.message}"
      rescue SocketError => e
        raise ConnectionError, "Network error: #{e.message}"
      rescue OpenSSL::SSL::SSLError => e
        raise ConnectionError, "SSL error: #{e.message}"
      rescue Net::ReadTimeout => e
        raise ConnectionError, "Request timeout: #{e.message}"
      rescue Net::OpenTimeout => e
        raise ConnectionError, "Connection timeout: #{e.message}"
      rescue => e
        # Catch any other network/HTTP errors
        raise ConnectionError, "Network error: #{e.class} - #{e.message}"
      end

      # Handle HTTP status codes
      case response.code.to_i
      when 200
        response.body
      when 400, 405  # 405 Method Not Allowed is treated as BadRequest
        error_message = extract_error_message(response.body)
        raise CommandError, error_message
      when 401
        error_message = extract_error_message(response.body)
        raise AuthenticationError, error_message
      when 403
        error_message = extract_error_message(response.body)
        raise AuthorizationError, error_message
      when 404
        error_message = extract_error_message(response.body)
        raise NotFoundError, error_message
      when 500
        error_message = extract_error_message(response.body)
        raise ServerError, error_message
      when 503
        error_message = extract_error_message(response.body)
        raise ConnectionError, error_message
      else
        # For other status codes, raise generic error with status code
        raise Error, "HTTP #{response.code}: #{response.body}"
      end
    end

    private

    # Extract error message from JSON error response
    def extract_error_message(body)
      begin
        parsed = JSON.parse(body)
        if parsed.is_a?(Hash) && parsed["message"]
          parsed["message"]
        else
          body.to_s.strip.empty? ? "Error occurred" : body.to_s
        end
      rescue JSON::ParserError
        body.to_s.strip.empty? ? "Error occurred" : body.to_s
      end
    end

    # Compute HMAC signature for authentication
    def compute_signature(message)
      digest = OpenSSL::Digest.new("sha256")
      trimmed_message = message.strip
      OpenSSL::HMAC.hexdigest(digest, @secret_key, trimmed_message)
    end
  end
end

