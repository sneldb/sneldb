require "uri"
require "json"
require_relative "errors"
require_relative "transport/http"
require_relative "transport/tcp"
require_relative "auth/manager"

# Optional Arrow support - only load if available
begin
  require "arrow"
  ARROW_AVAILABLE = true
rescue LoadError
  ARROW_AVAILABLE = false
end

module SnelDB
  class Client
    attr_reader :user_id, :secret_key, :output_format, :transport, :auth_manager, :base_url

    # Initialize a new SnelDB client
    #
    # @param base_url [String] The base URL of the SnelDB server (e.g., "http://localhost:8085" or "tcp://localhost:8086")
    # @param user_id [String, nil] Optional user ID for authentication
    # @param secret_key [String, nil] Optional secret key for authentication
    # @param output_format [String] Response format: "text", "json", or "arrow" (default: "text")
    # @param read_timeout [Integer] Read timeout in seconds (default: 60)
    # @param auto_initialize [Boolean] Automatically initialize database if not initialized (default: false)
    # @param initial_admin_user [String, nil] Admin user ID for initialization (from config: initial_admin_user)
    # @param initial_admin_key [String, nil] Admin secret key for initialization (from config: initial_admin_key)
    def initialize(base_url:, user_id: nil, secret_key: nil, output_format: "text", read_timeout: 60, auto_initialize: false, initial_admin_user: nil, initial_admin_key: nil)
      @base_url = base_url.chomp("/")
      @user_id = user_id
      @secret_key = secret_key
      @output_format = output_format
      @read_timeout = read_timeout
      @initial_admin_user = initial_admin_user
      @initial_admin_key = initial_admin_key

      # Parse URL and create appropriate transport
      uri = URI.parse(@base_url)
      case uri.scheme
      when "http", "https"
        @transport = Transport::HTTP.new(
          host: uri.host || "localhost",
          port: uri.port || (uri.scheme == "https" ? 443 : 80),
          use_ssl: uri.scheme == "https",
          read_timeout: @read_timeout
        )
      when "tcp", "tls"
        @transport = Transport::TCP.new(
          host: uri.host || "localhost",
          port: uri.port || 8086,
          use_ssl: uri.scheme == "tls",
          read_timeout: @read_timeout
        )
      else
        # Default to HTTP if scheme not specified
        @transport = Transport::HTTP.new(
          host: uri.host || "localhost",
          port: uri.port || 8085,
          use_ssl: false,
          read_timeout: @read_timeout
        )
      end

      # Create auth manager
      @auth_manager = Auth::Manager.new(user_id: @user_id, secret_key: @secret_key)

      # Auto-initialize if requested
      if auto_initialize
        ensure_initialized!
      end
    end

    # Execute a raw command string (non-raising version)
    #
    # @param command [String] The command string to execute
    # @return [Hash] Result hash with :success (boolean), :data (Array<Hash>), :error (Error or nil)
    def execute(command)
      begin
        data = execute!(command)
        { success: true, data: data, error: nil }
      rescue Error => e
        { success: false, data: nil, error: e }
      rescue => e
        # Wrap unexpected errors
        { success: false, data: nil, error: Error.new("Unexpected error: #{e.class} - #{e.message}") }
      end
    end

    # Execute a raw command string (raising version)
    #
    # @param command [String] The command string to execute
    # @return [Array<Hash>] The response as an array of hashes (normalized format)
    # @raise [SnelDB::ConnectionError] if network/connection errors occur
    # @raise [SnelDB::AuthenticationError] if authentication fails (401)
    # @raise [SnelDB::AuthorizationError] if authorization fails (403)
    # @raise [SnelDB::CommandError] if the command is invalid (400)
    # @raise [SnelDB::NotFoundError] if resource not found (404)
    # @raise [SnelDB::ServerError] if server error occurs (500)
    # @raise [SnelDB::ParseError] if response parsing fails
    # @raise [SnelDB::Error] for other errors
    def execute!(command)
      # Format command with authentication based on transport type
      formatted_command = if @transport.is_a?(Transport::TCP)
        @auth_manager.format_tcp_command(command, @transport)
      else
        command
      end

      # Prepare headers
      headers = {}
      if @transport.is_a?(Transport::HTTP)
        headers = @auth_manager.add_http_headers(formatted_command, headers: headers)
      end

      # Execute via transport
      response = @transport.execute(formatted_command, headers: headers)

      # Handle response status codes
      case response[:status]
      when 200
        # Parse response and normalize to array of hashes
        begin
          parse_and_normalize_response(response)
        rescue => e
          raise ParseError, "Failed to parse response: #{e.message}"
        end
      when 400, 405  # 405 Method Not Allowed is treated as BadRequest
        error_message = extract_error_message(response[:body])
        raise CommandError, error_message
      when 401
        error_message = extract_error_message(response[:body])
        raise AuthenticationError, error_message
      when 403
        error_message = extract_error_message(response[:body])
        raise AuthorizationError, error_message
      when 404
        error_message = extract_error_message(response[:body])
        raise NotFoundError, error_message
      when 500
        error_message = extract_error_message(response[:body])
        raise ServerError, error_message
      when 503
        error_message = extract_error_message(response[:body])
        raise ConnectionError, error_message
      else
        # For other status codes, raise generic error with status code
        raise Error, "HTTP #{response[:status]}: #{response[:body]}"
      end
    end

    # Authenticate using AUTH command (for TCP/WebSocket connections)
    # This establishes a session token for high-throughput authentication
    # @return [String] Session token
    # @raise [SnelDB::AuthenticationError] if authentication fails
    def authenticate!
      unless @transport.is_a?(Transport::TCP)
        raise AuthenticationError, "AUTH command is only supported for TCP connections"
      end
      @auth_manager.authenticate!(@transport)
    end

    # Close the connection (for TCP connections)
    def close
      @transport.close if @transport.respond_to?(:close)
    end

    # Ensure database is initialized by checking if users exist and creating admin user if needed
    # This is useful for first-time setup or when the database needs to be initialized
    #
    # This method works when:
    # - Authentication is bypassed (bypass_auth = true in server config)
    # - Or when the server allows user creation without authentication when no users exist
    #
    # @param admin_user_id [String, nil] Admin user ID (defaults to @initial_admin_user or "admin")
    # @param admin_secret_key [String, nil] Admin secret key (defaults to @initial_admin_key)
    # @return [Boolean] true if initialization was performed, false if already initialized
    # @raise [SnelDB::Error] if initialization fails
    def ensure_initialized!(admin_user_id: nil, admin_secret_key: nil)
      admin_user = admin_user_id || @initial_admin_user || "admin"
      admin_key = admin_secret_key || @initial_admin_key

      if admin_key.nil?
        raise Error, "Admin secret key required for initialization. Provide initial_admin_key parameter or set it in config."
      end

      # First, try to check if database is initialized by listing users
      # Use a client without auth credentials to check if auth is bypassed
      check_client = self.class.new(
        base_url: @base_url,
        output_format: @output_format,
        read_timeout: @read_timeout,
        auto_initialize: false
      )

      begin
        result = check_client.list_users
        if result[:success] && result[:data] && result[:data].any?
          # Users exist, database is initialized
          return false
        end
      rescue AuthenticationError, AuthorizationError
        # Auth required but no users exist, proceed with initialization
        # Try creating user without authentication (works if bypass_auth = true)
      rescue
        # Other errors - might be connection issues, but try initialization anyway
      end

      # Try to create admin user without authentication first (for bypass_auth mode)
      begin
        result = check_client.create_user(user_id: admin_user, secret_key: admin_key)

        if result[:success]
          # Update this client's credentials if they weren't set
          if @user_id.nil? && @secret_key.nil?
            @user_id = admin_user
            @secret_key = admin_key
            @auth_manager = Auth::Manager.new(user_id: @user_id, secret_key: @secret_key)
          end
          return true
        else
          # Check if user already exists (which means DB is initialized)
          if result[:error].is_a?(CommandError) && result[:error].message.include?("already exists")
            return false
          end
          raise result[:error] || Error.new("Failed to initialize database")
        end
      rescue AuthenticationError, AuthorizationError
        # Auth required - can't create user without authentication
        # This means either:
        # 1. Auth is not bypassed and users already exist (DB is initialized)
        # 2. Auth is required but no users exist (server should bootstrap on startup)
        # Try one more time with auth to see if user exists
        temp_client = self.class.new(
          base_url: @base_url,
          user_id: admin_user,
          secret_key: admin_key,
          output_format: @output_format,
          read_timeout: @read_timeout,
          auto_initialize: false
        )

        begin
          result = temp_client.list_users
          if result[:success]
            return false # Users exist, already initialized
          end
        rescue
          # If listing fails even with auth, the server might need to bootstrap on startup
          raise Error, "Cannot initialize database: authentication required but no users exist. " \
                       "The server should automatically bootstrap the admin user on startup if " \
                       "initial_admin_user and initial_admin_key are configured in prod.toml."
        end
      end
    end

    # Check if database is initialized (has at least one user)
    #
    # @return [Boolean] true if database appears initialized, false otherwise
    def initialized?
      begin
        result = list_users
        result[:success] && result[:data] && result[:data].any?
      rescue AuthenticationError, AuthorizationError
        # If we get auth errors, assume not initialized
        false
      rescue
        # For other errors, assume initialized (to avoid false positives)
        true
      end
    end


    # Define a schema for an event type (non-raising)
    #
    # @param event_type [String] The event type name
    # @param fields [Hash] Field definitions (e.g., { "id" => "int", "name" => "string", "plan" => ["pro", "basic"] })
    # @param version [Integer, nil] Optional schema version
    # @return [Hash] Result hash with :success, :data, :error
    def define(event_type:, fields:, version: nil)
      schema = build_fields_json(fields)

      command = if version
        "DEFINE #{event_type} AS #{version} FIELDS #{schema}"
      else
        "DEFINE #{event_type} FIELDS #{schema}"
      end

      execute(command)
    end

    # Define a schema for an event type (raising)
    #
    # @param event_type [String] The event type name
    # @param fields [Hash] Field definitions (e.g., { "id" => "int", "name" => "string", "plan" => ["pro", "basic"] })
    # @param version [Integer, nil] Optional schema version
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def define!(event_type:, fields:, version: nil)
      schema = build_fields_json(fields)

      command = if version
        "DEFINE #{event_type} AS #{version} FIELDS #{schema}"
      else
        "DEFINE #{event_type} FIELDS #{schema}"
      end

      execute!(command)
    end

    # Store an event (non-raising)
    #
    # @param event_type [String] The event type name
    # @param context_id [String] The context ID
    # @param payload [Hash] The event payload (will be converted to JSON)
    # @return [Hash] Result hash with :success, :data, :error
    def store(event_type:, context_id:, payload:)
      payload_json = payload.to_json

      # Handle context_id that might need quoting
      context_str = if context_id.match?(/^[a-zA-Z0-9_-]+$/)
        context_id
      else
        "\"#{context_id}\""
      end

      command = "STORE #{event_type} FOR #{context_str} PAYLOAD #{payload_json}"
      execute(command)
    end

    # Store an event (raising)
    #
    # @param event_type [String] The event type name
    # @param context_id [String] The context ID
    # @param payload [Hash] The event payload (will be converted to JSON)
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def store!(event_type:, context_id:, payload:)
      payload_json = payload.to_json

      # Handle context_id that might need quoting
      context_str = if context_id.match?(/^[a-zA-Z0-9_-]+$/)
        context_id
      else
        "\"#{context_id}\""
      end

      command = "STORE #{event_type} FOR #{context_str} PAYLOAD #{payload_json}"
      execute!(command)
    end

    # Query events (non-raising)
    #
    # @param event_type [String] The event type to query
    # @param context_id [String, nil] Optional context ID filter
    # @param since [String, Integer, nil] Optional timestamp filter (ISO-8601 string or epoch)
    # @param using [String, nil] Optional time field name
    # @param where [String, nil] Optional WHERE clause
    # @param limit [Integer, nil] Optional result limit
    # @param return_fields [Array<String>, nil] Optional fields to return
    # @return [Hash] Result hash with :success, :data, :error
    def query(event_type:, context_id: nil, since: nil, using: nil, where: nil, limit: nil, return_fields: nil)
      parts = ["QUERY #{event_type}"]

      parts << "FOR #{quote_if_needed(context_id)}" if context_id
      parts << "SINCE #{quote_if_needed(since)}" if since
      parts << "USING #{using}" if using

      if return_fields && !return_fields.empty?
        fields_str = return_fields.map { |f| quote_if_needed(f) }.join(", ")
        parts << "RETURN [#{fields_str}]"
      end

      parts << "WHERE #{where}" if where
      parts << "LIMIT #{limit}" if limit

      execute(parts.join(" "))
    end

    # Query events (raising)
    #
    # @param event_type [String] The event type to query
    # @param context_id [String, nil] Optional context ID filter
    # @param since [String, Integer, nil] Optional timestamp filter (ISO-8601 string or epoch)
    # @param using [String, nil] Optional time field name
    # @param where [String, nil] Optional WHERE clause
    # @param limit [Integer, nil] Optional result limit
    # @param return_fields [Array<String>, nil] Optional fields to return
    # @return [Array<Hash>] Array of event hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def query!(event_type:, context_id: nil, since: nil, using: nil, where: nil, limit: nil, return_fields: nil)
      parts = ["QUERY #{event_type}"]

      parts << "FOR #{quote_if_needed(context_id)}" if context_id
      parts << "SINCE #{quote_if_needed(since)}" if since
      parts << "USING #{using}" if using

      if return_fields && !return_fields.empty?
        fields_str = return_fields.map { |f| quote_if_needed(f) }.join(", ")
        parts << "RETURN [#{fields_str}]"
      end

      parts << "WHERE #{where}" if where
      parts << "LIMIT #{limit}" if limit

      execute!(parts.join(" "))
    end

    # Replay events for a context (non-raising)
    #
    # @param context_id [String] The context ID
    # @param event_type [String, nil] Optional event type filter
    # @param since [String, Integer, nil] Optional timestamp filter
    # @param using [String, nil] Optional time field name
    # @param return_fields [Array<String>, nil] Optional fields to return
    # @return [Hash] Result hash with :success, :data, :error
    def replay(context_id:, event_type: nil, since: nil, using: nil, return_fields: nil)
      parts = ["REPLAY"]

      parts << event_type if event_type
      parts << "FOR #{quote_if_needed(context_id)}"
      parts << "SINCE #{quote_if_needed(since)}" if since
      parts << "USING #{using}" if using

      if return_fields && !return_fields.empty?
        fields_str = return_fields.map { |f| quote_if_needed(f) }.join(", ")
        parts << "RETURN [#{fields_str}]"
      end

      execute(parts.join(" "))
    end

    # Replay events for a context (raising)
    #
    # @param context_id [String] The context ID
    # @param event_type [String, nil] Optional event type filter
    # @param since [String, Integer, nil] Optional timestamp filter
    # @param using [String, nil] Optional time field name
    # @param return_fields [Array<String>, nil] Optional fields to return
    # @return [Array<Hash>] Array of event hashes in chronological order
    # @raise [SnelDB::Error] (see #execute! for error types)
    def replay!(context_id:, event_type: nil, since: nil, using: nil, return_fields: nil)
      parts = ["REPLAY"]

      parts << event_type if event_type
      parts << "FOR #{quote_if_needed(context_id)}"
      parts << "SINCE #{quote_if_needed(since)}" if since
      parts << "USING #{using}" if using

      if return_fields && !return_fields.empty?
        fields_str = return_fields.map { |f| quote_if_needed(f) }.join(", ")
        parts << "RETURN [#{fields_str}]"
      end

      execute!(parts.join(" "))
    end

    # Flush memtable to disk (non-raising)
    #
    # @return [Hash] Result hash with :success, :data, :error
    def flush
      execute("FLUSH")
    end

    # Flush memtable to disk (raising)
    #
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def flush!
      execute!("FLUSH")
    end

    # Ping the server (non-raising)
    #
    # @return [Hash] Result hash with :success, :data, :error
    def ping
      execute("PING")
    end

    # Ping the server (raising)
    #
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def ping!
      execute!("PING")
    end

    # Create a user (non-raising)
    #
    # Requires admin authentication. Creates a regular user without roles.
    # To grant permissions, use #grant_permission after creating the user.
    #
    # @param user_id [String] The user ID to create
    # @param secret_key [String, nil] Optional secret key (generated if not provided)
    # @return [Hash] Result hash with :success, :data, :error
    # @example
    #   # Create a user with auto-generated key
    #   result = client.create_user(user_id: "api_client")
    #   secret_key = result[:data].find { |line| line[:raw].include?("Secret key:") }
    #
    #   # Create a user with custom key
    #   client.create_user(user_id: "service_account", secret_key: "my_secret_key")
    def create_user(user_id:, secret_key: nil)
      command = if secret_key
        # Quote secret key if it contains special characters
        quoted_key = if secret_key.match?(/^[a-zA-Z0-9_-]+$/)
          secret_key
        else
          "\"#{secret_key}\""
        end
        "CREATE USER #{user_id} WITH KEY #{quoted_key}"
      else
        "CREATE USER #{user_id}"
      end
      execute(command)
    end

    # Create a user (raising)
    #
    # Requires admin authentication. Creates a regular user without roles.
    # To grant permissions, use #grant_permission after creating the user.
    #
    # @param user_id [String] The user ID to create
    # @param secret_key [String, nil] Optional secret key (generated if not provided)
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    # @example
    #   # Create a user and get the generated secret key
    #   result = client.create_user!(user_id: "api_client")
    #   # Response contains: ["User 'api_client' created", "Secret key: abc123..."]
    def create_user!(user_id:, secret_key: nil)
      command = if secret_key
        # Quote secret key if it contains special characters
        quoted_key = if secret_key.match?(/^[a-zA-Z0-9_-]+$/)
          secret_key
        else
          "\"#{secret_key}\""
        end
        "CREATE USER #{user_id} WITH KEY #{quoted_key}"
      else
        "CREATE USER #{user_id}"
      end
      execute!(command)
    end


    # List all users (non-raising)
    #
    # @return [Hash] Result hash with :success, :data, :error
    def list_users
      execute("LIST USERS")
    end

    # List all users (raising)
    #
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def list_users!
      execute!("LIST USERS")
    end

    # Revoke a user's key (non-raising)
    #
    # @param user_id [String] The user ID
    # @return [Hash] Result hash with :success, :data, :error
    def revoke_key(user_id:)
      execute("REVOKE KEY #{user_id}")
    end

    # Revoke a user's key (raising)
    #
    # @param user_id [String] The user ID
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def revoke_key!(user_id:)
      execute!("REVOKE KEY #{user_id}")
    end

    # Grant permissions to a user for event types (non-raising)
    #
    # @param user_id [String] The user ID
    # @param permissions [Array<String>] Permissions to grant: ["read"], ["write"], or ["read", "write"]
    # @param event_types [Array<String>] Event types to grant permissions for
    # @return [Hash] Result hash with :success, :data, :error
    def grant_permission(user_id:, permissions:, event_types:)
      perms_str = permissions.map(&:upcase).join(", ")
      event_types_str = event_types.map { |et| quote_if_needed(et) }.join(", ")
      command = "GRANT #{perms_str} ON #{event_types_str} TO #{user_id}"
      execute(command)
    end

    # Grant permissions to a user for event types (raising)
    #
    # @param user_id [String] The user ID
    # @param permissions [Array<String>] Permissions to grant: ["read"], ["write"], or ["read", "write"]
    # @param event_types [Array<String>] Event types to grant permissions for
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def grant_permission!(user_id:, permissions:, event_types:)
      perms_str = permissions.map(&:upcase).join(", ")
      event_types_str = event_types.map { |et| quote_if_needed(et) }.join(", ")
      command = "GRANT #{perms_str} ON #{event_types_str} TO #{user_id}"
      execute!(command)
    end

    # Revoke permissions from a user for event types (non-raising)
    #
    # @param user_id [String] The user ID
    # @param permissions [Array<String>, nil] Permissions to revoke: ["read"], ["write"], or ["read", "write"]. If nil, revokes all permissions.
    # @param event_types [Array<String>] Event types to revoke permissions for
    # @return [Hash] Result hash with :success, :data, :error
    def revoke_permission(user_id:, permissions: nil, event_types:)
      perms_str = if permissions && !permissions.empty?
        permissions.map(&:upcase).join(", ")
      else
        ""
      end
      event_types_str = event_types.map { |et| quote_if_needed(et) }.join(", ")
      command = if perms_str.empty?
        "REVOKE ON #{event_types_str} FROM #{user_id}"
      else
        "REVOKE #{perms_str} ON #{event_types_str} FROM #{user_id}"
      end
      execute(command)
    end

    # Revoke permissions from a user for event types (raising)
    #
    # @param user_id [String] The user ID
    # @param permissions [Array<String>, nil] Permissions to revoke: ["read"], ["write"], or ["read", "write"]. If nil, revokes all permissions.
    # @param event_types [Array<String>] Event types to revoke permissions for
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def revoke_permission!(user_id:, permissions: nil, event_types:)
      perms_str = if permissions && !permissions.empty?
        permissions.map(&:upcase).join(", ")
      else
        ""
      end
      event_types_str = event_types.map { |et| quote_if_needed(et) }.join(", ")
      command = if perms_str.empty?
        "REVOKE ON #{event_types_str} FROM #{user_id}"
      else
        "REVOKE #{perms_str} ON #{event_types_str} FROM #{user_id}"
      end
      execute!(command)
    end

    # Show permissions for a user (non-raising)
    #
    # @param user_id [String] The user ID
    # @return [Hash] Result hash with :success, :data, :error
    def show_permissions(user_id:)
      execute("SHOW PERMISSIONS FOR #{user_id}")
    end

    # Show permissions for a user (raising)
    #
    # @param user_id [String] The user ID
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def show_permissions!(user_id:)
      execute!("SHOW PERMISSIONS FOR #{user_id}")
    end

    # Remember a query as a materialized view (NOT IMPLEMENTED YET)
    #
    # @param name [String] The name of the materialized view
    # @param event_type [String] The event type to query
    # @param context_id [String, nil] Optional context ID filter
    # @param since [String, Integer, nil] Optional timestamp filter
    # @param using [String, nil] Optional time field name
    # @param where [String, nil] Optional WHERE clause
    # @param limit [Integer, nil] Optional result limit
    # @param return_fields [Array<String>, nil] Optional fields to return
    # @return [Hash] Result hash with :success, :data, :error
    def remember(name:, event_type:, context_id: nil, since: nil, using: nil, where: nil, limit: nil, return_fields: nil)
      raise NotImplementedError, "REMEMBER command is not yet implemented"
    end

    # Remember a query as a materialized view (NOT IMPLEMENTED YET)
    #
    # @param name [String] The name of the materialized view
    # @param event_type [String] The event type to query
    # @param context_id [String, nil] Optional context ID filter
    # @param since [String, Integer, nil] Optional timestamp filter
    # @param using [String, nil] Optional time field name
    # @param where [String, nil] Optional WHERE clause
    # @param limit [Integer, nil] Optional result limit
    # @param return_fields [Array<String>, nil] Optional fields to return
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def remember!(name:, event_type:, context_id: nil, since: nil, using: nil, where: nil, limit: nil, return_fields: nil)
      raise NotImplementedError, "REMEMBER command is not yet implemented"
    end

    # Show a materialized view (NOT IMPLEMENTED YET)
    #
    # @param name [String] The name of the materialized view
    # @return [Hash] Result hash with :success, :data, :error
    def show_materialized(name:)
      raise NotImplementedError, "SHOW MATERIALIZED command is not yet implemented"
    end

    # Show a materialized view (NOT IMPLEMENTED YET)
    #
    # @param name [String] The name of the materialized view
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def show_materialized!(name:)
      raise NotImplementedError, "SHOW MATERIALIZED command is not yet implemented"
    end

    # Execute a batch of commands (NOT IMPLEMENTED YET)
    #
    # @param commands [Array<String>] Array of command strings
    # @return [Hash] Result hash with :success, :data, :error
    def batch(commands)
      raise NotImplementedError, "BATCH command is not yet implemented"
    end

    # Execute a batch of commands (NOT IMPLEMENTED YET)
    #
    # @param commands [Array<String>] Array of command strings
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def batch!(commands)
      raise NotImplementedError, "BATCH command is not yet implemented"
    end

    # Compare multiple queries (NOT IMPLEMENTED YET)
    #
    # @param queries [Array<String>] Array of query command strings
    # @return [Hash] Result hash with :success, :data, :error
    def compare(queries)
      raise NotImplementedError, "COMPARE command is not yet implemented"
    end

    # Compare multiple queries (NOT IMPLEMENTED YET)
    #
    # @param queries [Array<String>] Array of query command strings
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def compare!(queries)
      raise NotImplementedError, "COMPARE command is not yet implemented"
    end

    # Execute a PLOT query (NOT IMPLEMENTED YET)
    #
    # @param query [String] The PLOT query string
    # @return [Hash] Result hash with :success, :data, :error
    def plot(query)
      raise NotImplementedError, "PLOT command is not yet implemented"
    end

    # Execute a PLOT query (NOT IMPLEMENTED YET)
    #
    # @param query [String] The PLOT query string
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def plot!(query)
      raise NotImplementedError, "PLOT command is not yet implemented"
    end

    private

    # Build JSON string for FIELDS in DEFINE command
    # Handles both primitive types (strings) and enum types (arrays)
    # @param fields [Hash] Field definitions
    # @return [String] JSON string for the fields
    def build_fields_json(fields)
      fields_array = fields.map do |k, v|
        if v.is_a?(Array)
          # Enum field: convert array to JSON array
          enum_json = v.map { |variant| "\"#{variant}\"" }.join(", ")
          "\"#{k}\": [#{enum_json}]"
        else
          # Primitive field: string type
          "\"#{k}\": \"#{v}\""
        end
      end
      "{ #{fields_array.join(", ")} }"
    end

    # Extract error message from JSON error response
    # @param body [String] The response body
    # @return [String] The error message
    def extract_error_message(body)
      begin
        parsed = JSON.parse(body)
        if parsed.is_a?(Hash) && parsed["message"]
          parsed["message"]
        else
          # Fallback: use the body as-is or a default message
          body.to_s.strip.empty? ? "Error occurred" : body.to_s
        end
      rescue JSON::ParserError
        # If not JSON, return the body as-is
        body.to_s.strip.empty? ? "Error occurred" : body.to_s
      end
    end

    # Parse and normalize response to always return array of hashes
    def parse_and_normalize_response(response)
      # Handle both HTTP response objects and hash responses from transport
      if response.is_a?(Hash)
        content_type = response[:headers] && response[:headers]["content-type"] ? response[:headers]["content-type"].first : ""
        body = response[:body]
      else
        content_type = response["Content-Type"] || ""
        body = response.body
      end

      # If content type is explicitly JSON, don't try Arrow parsing
      if content_type.include?("application/json")
        return parse_text_to_hashes(body)
      end

      # Check if response is Arrow format
      # Note: Error responses may return 200 OK with JSON error messages
      # even when output_format is "arrow", so we need to handle both cases
      is_arrow = (@output_format == "arrow") ||
                 (content_type.include?("arrow") && !content_type.include?("json")) ||
                 content_type.include?("application/vnd.apache.arrow")

      if ARROW_AVAILABLE && is_arrow
        # Try to parse as Arrow first, but fall back to text if it fails
        # This handles cases where server returns 200 OK with JSON error messages
        begin
          parse_arrow_to_hashes(body)
        rescue
          # If Arrow parsing fails, it might be a JSON error message
          # Fall back to text parsing
          parse_text_to_hashes(body)
        end
      else
        # Parse text format and convert to array of hashes
        parse_text_to_hashes(body)
      end
    end

    # Parse Arrow IPC format and return as array of hashes
    def parse_arrow_to_hashes(arrow_data)
      unless ARROW_AVAILABLE
        raise ParseError, "Arrow gem not available. Install 'red-arrow' gem to parse Arrow format."
      end

      begin
        # Create a buffer from the binary data
        buffer = Arrow::Buffer.new(arrow_data)
        input_stream = Arrow::BufferInputStream.new(buffer)

        # Use RecordBatchStreamReader to read the Arrow IPC stream
        reader = Arrow::RecordBatchStreamReader.new(input_stream)

        records = []

        # Read all record batches from the stream
        loop do
          begin
            record_batch = reader.read_next
            break if record_batch.nil?

            # Get schema and column information
            schema = record_batch.schema
            num_rows = record_batch.n_rows
            num_columns = record_batch.n_columns

            # Convert each row to a hash
            num_rows.times do |row_idx|
              record = {}
              num_columns.times do |col_idx|
                field = schema.get_field(col_idx)
                field_name = field.name
                field_type = field.data_type
                column = record_batch[col_idx]

                # For timestamp fields, get raw value before conversion
                # Server stores seconds but sends as TimestampMillisecond
                # red-arrow will interpret as milliseconds, so we need raw value
                if field_type.to_s.downcase.include?("timestamp")
                  # Column might be Arrow::Column, need to access underlying data array
                  data_array = column.respond_to?(:data) ? column.data : column

                  # Use get_raw_value to get the raw Int64 value before conversion
                  # Server stores Unix timestamps in seconds, but Arrow field type is TimestampMillisecond
                  # The raw value in the Arrow array is the seconds value (as Int64)
                  raw_value = data_array.get_raw_value(row_idx)

                  # Treat raw value as seconds (server stores seconds, not milliseconds)
                  value = Time.at(raw_value)
                  record[field_name] = value.iso8601
                else
                  value = column[row_idx]
                  record[field_name] = convert_arrow_value(value, field_type, field_name)
                end
              end
              records << record
            end
          rescue StopIteration, EOFError
            break
          rescue => e
            # If we have some records, return them; otherwise re-raise
            if records.any?
              break
            else
              raise ParseError, "Failed to read Arrow record batch: #{e.message}"
            end
          end
        end

        records
      rescue ParseError
        # Re-raise ParseError as-is
        raise
      rescue => e
        raise ParseError, "Failed to parse Arrow format: #{e.message}"
      end
    end

    # Parse text format response and convert to array of hashes
    def parse_text_to_hashes(text_data)
      begin
        # Ensure UTF-8 encoding
        text = text_data.to_s.force_encoding('UTF-8')
        text = text.encode('UTF-8', 'UTF-8', invalid: :replace, undef: :replace)
      rescue => e
        raise ParseError, "Failed to encode response as UTF-8: #{e.message}"
      end

      # Try to parse as JSON first
      # Only attempt JSON parsing if it looks like valid JSON (starts with { or [ and has matching brackets)
      stripped = text.strip
      if stripped.start_with?('{') && stripped.end_with?('}')
        begin
          parsed = JSON.parse(text)
          return parsed if parsed.is_a?(Array)
          return [parsed] if parsed.is_a?(Hash)
        rescue JSON::ParserError => e
          # If it looks like JSON object but fails to parse, check if it's clearly malformed JSON
          # (has quotes, colons, etc.) vs just text that happens to start/end with braces
          if text.match?(/["':]/) && text.length < 1000
            # Looks like it was meant to be JSON but is malformed
            raise ParseError, "Invalid JSON response: #{e.message}"
          end
          # Otherwise, fall through to text parsing
        end
      elsif stripped.start_with?('[') && stripped.end_with?(']')
        begin
          parsed = JSON.parse(text)
          return parsed if parsed.is_a?(Array)
          return [parsed] if parsed.is_a?(Hash)
        rescue JSON::ParserError => e
          # If it looks like JSON array but fails to parse, check if it's clearly malformed JSON
          if text.match?(/["':]/) && text.length < 1000
            # Looks like it was meant to be JSON but is malformed
            raise ParseError, "Invalid JSON response: #{e.message}"
          end
          # Otherwise, fall through to text parsing
        end
      end

      # Parse as text format (pipe-delimited or line-by-line)
      begin
        lines = text.split("\n").reject { |line| line.nil? || line.strip.empty? }
        return [] if lines.empty?

        # Check if it's pipe-delimited format
        if lines.first.include?("|")
          parse_pipe_delimited(lines)
        else
          # Simple line-by-line format
          lines.map { |line| { raw: line.strip } }
        end
      rescue => e
        raise ParseError, "Failed to parse text response: #{e.message}"
      end
    end

    # Parse pipe-delimited text format
    def parse_pipe_delimited(lines)
      # Try to detect headers from first line
      first_line = lines.first
      headers = first_line.split("|").map(&:strip)

      # If first line looks like headers (all caps or specific pattern), use it
      # Otherwise, treat all lines as data
      data_start = 0
      if headers.all? { |h| h.match?(/^[A-Z_]+$/) } && lines.length > 1
        data_start = 1
      end

      lines[data_start..-1].map do |line|
        values = line.split("|").map(&:strip)
        if data_start > 0 && values.length == headers.length
          # Create hash from headers and values
          headers.each_with_index.each_with_object({}) do |(header, idx), hash|
            hash[header.downcase] = values[idx] if idx < values.length
          end
        else
          { raw: line.strip, parts: values }
        end
      end
    end

    # Convert Arrow value to Ruby native type
    # @param value [Object] The value from Arrow column
    # @param field_type [Arrow::DataType] The Arrow data type of the field
    # @param field_name [String] The name of the field
    def convert_arrow_value(value, field_type = nil, field_name = nil)
      return nil if value.nil?

      # Handle non-timestamp types
      # Note: Timestamps are handled directly in the parsing loop
      case value
      when String
        value
      when Numeric
        value
      when TrueClass, FalseClass
        value
      when Time
        value.iso8601
      when Date, DateTime
        value.to_s
      else
        value.to_s
      end
    end


    # Quote a value if it contains special characters or spaces
    def quote_if_needed(value)
      case value
      when String
        # If it's already quoted or is a simple word, return as-is
        if value.match?(/^"[^"]*"$/) || value.match?(/^[a-zA-Z0-9_-]+$/)
          value
        else
          "\"#{value}\""
        end
      when Integer, Float
        value.to_s
      else
        "\"#{value}\""
      end
    end
  end
end

