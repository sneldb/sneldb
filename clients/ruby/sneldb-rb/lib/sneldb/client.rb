require "net/http"
require "uri"
require "json"
require "openssl"
require_relative "errors"
require_relative "tcp_transport"
require_relative "http_transport"

# Optional Arrow support - only load if available
begin
  require "arrow"
  ARROW_AVAILABLE = true
rescue LoadError
  ARROW_AVAILABLE = false
end

module SnelDB
  class Client
    attr_reader :user_id, :secret_key, :output_format, :protocol, :address

    # Initialize a new SnelDB client
    #
    # @param address [String] Server address. For TCP: "localhost:8086" or "host:port". For HTTP: "http://localhost:8085" or "https://host:port"
    # @param protocol [String] Protocol to use: "tcp" (default) or "http"
    # @param user_id [String, nil] Optional user ID for authentication
    # @param secret_key [String, nil] Optional secret key for authentication
    # @param output_format [String] Response format: "text", "json", or "arrow" (default: "text")
    def initialize(address:, protocol: nil, user_id: nil, secret_key: nil, output_format: "text")
      @address = address

      # Auto-detect protocol from address if not specified
      if protocol.nil?
        if address.start_with?("http://") || address.start_with?("https://")
          @protocol = "http"
        else
          @protocol = "tcp"
        end
      else
        @protocol = protocol.downcase
      end

      @user_id = user_id
      @secret_key = secret_key
      @output_format = output_format

      # Initialize transport based on protocol
      @transport = create_transport
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
      # Execute command using the appropriate transport
      response_body = @transport.execute(command)

      # Parse response and normalize to array of hashes
      begin
        parse_and_normalize_response(response_body)
        rescue => e
          raise ParseError, "Failed to parse response: #{e.message}"
        end
    end

    # Close the connection (for TCP transport)
    def close
      if @transport.respond_to?(:close)
        @transport.close
      end
    end


    # Define a schema for an event type (non-raising)
    #
    # @param event_type [String] The event type name
    # @param fields [Hash] Field definitions (e.g., { "id" => "int", "name" => "string", "plan" => ["pro", "basic"] })
    # @param version [Integer, nil] Optional schema version
    # @param auto_grant [Boolean] Automatically grant read/write permissions to current user (default: true)
    # @return [Hash] Result hash with :success, :data, :error
    def define(event_type:, fields:, version: nil, auto_grant: true)
      schema = build_fields_json(fields)

      command = if version
        "DEFINE #{event_type} AS #{version} FIELDS #{schema}"
      else
        "DEFINE #{event_type} FIELDS #{schema}"
      end

      result = execute(command)

      # Automatically grant permissions if define was successful and user is authenticated
      if result[:success] && auto_grant && @user_id
        grant_result = grant_permission(
          permissions: ["read", "write"],
          event_types: [event_type],
          user_id: @user_id
        )
        # Log warning if grant fails, but don't fail the define operation
        if !grant_result[:success]
          # Silently ignore - permissions might already be granted or user might not have admin rights
        end
      end

      result
    end

    # Define a schema for an event type (raising)
    #
    # @param event_type [String] The event type name
    # @param fields [Hash] Field definitions (e.g., { "id" => "int", "name" => "string", "plan" => ["pro", "basic"] })
    # @param version [Integer, nil] Optional schema version
    # @param auto_grant [Boolean] Automatically grant read/write permissions to current user (default: true)
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def define!(event_type:, fields:, version: nil, auto_grant: true)
      schema = build_fields_json(fields)

      command = if version
        "DEFINE #{event_type} AS #{version} FIELDS #{schema}"
      else
        "DEFINE #{event_type} FIELDS #{schema}"
      end

      result = execute!(command)

      # Automatically grant permissions if user is authenticated
      if auto_grant && @user_id
        begin
          grant_permission!(
            permissions: ["read", "write"],
            event_types: [event_type],
            user_id: @user_id
          )
        rescue Error => e
          # Silently ignore - permissions might already be granted or user might not have admin rights
          # The define operation succeeded, which is what matters
        end
      end

      result
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


    # Create a user (non-raising)
    #
    # @param user_id [String] The user ID to create
    # @param secret_key [String, nil] Optional secret key (generated if not provided)
    # @return [Hash] Result hash with :success, :data, :error
    def create_user(user_id:, secret_key: nil)
      command = if secret_key
        # Quote secret key if it contains special characters
        key_str = quote_if_needed(secret_key)
        "CREATE USER #{quote_if_needed(user_id)} WITH KEY #{key_str}"
      else
        "CREATE USER #{quote_if_needed(user_id)}"
      end
      execute(command)
    end

    # Create a user (raising)
    #
    # @param user_id [String] The user ID to create
    # @param secret_key [String, nil] Optional secret key (generated if not provided)
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def create_user!(user_id:, secret_key: nil)
      command = if secret_key
        # Quote secret key if it contains special characters
        key_str = quote_if_needed(secret_key)
        "CREATE USER #{quote_if_needed(user_id)} WITH KEY #{key_str}"
      else
        "CREATE USER #{quote_if_needed(user_id)}"
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
      execute("REVOKE KEY #{quote_if_needed(user_id)}")
    end

    # Revoke a user's key (raising)
    #
    # @param user_id [String] The user ID
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def revoke_key!(user_id:)
      execute!("REVOKE KEY #{quote_if_needed(user_id)}")
    end

    # Grant permissions to a user for event types (non-raising)
    #
    # @param permissions [Array<String>] Array of permissions: ["read"], ["write"], or ["read", "write"]
    # @param event_types [Array<String>] Array of event type names
    # @param user_id [String] The user ID to grant permissions to
    # @return [Hash] Result hash with :success, :data, :error
    def grant_permission(permissions:, event_types:, user_id:)
      perms_str = permissions.map(&:upcase).join(",")
      event_types_str = event_types.map { |et| quote_if_needed(et) }.join(",")
      command = "GRANT #{perms_str} ON #{event_types_str} TO #{quote_if_needed(user_id)}"
      execute(command)
    end

    # Grant permissions to a user for event types (raising)
    #
    # @param permissions [Array<String>] Array of permissions: ["read"], ["write"], or ["read", "write"]
    # @param event_types [Array<String>] Array of event type names
    # @param user_id [String] The user ID to grant permissions to
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def grant_permission!(permissions:, event_types:, user_id:)
      perms_str = permissions.map(&:upcase).join(",")
      event_types_str = event_types.map { |et| quote_if_needed(et) }.join(",")
      command = "GRANT #{perms_str} ON #{event_types_str} TO #{quote_if_needed(user_id)}"
      execute!(command)
    end

    # Revoke permissions from a user for event types (non-raising)
    #
    # @param event_types [Array<String>] Array of event type names
    # @param user_id [String] The user ID to revoke permissions from
    # @param permissions [Array<String>, nil] Optional array of permissions to revoke. If nil, revokes all permissions.
    # @return [Hash] Result hash with :success, :data, :error
    def revoke_permission(event_types:, user_id:, permissions: nil)
      perms_str = if permissions && !permissions.empty?
        permissions.map(&:upcase).join(",")
      else
        ""
      end
      event_types_str = event_types.map { |et| quote_if_needed(et) }.join(",")
      command = if perms_str.empty?
        "REVOKE ON #{event_types_str} FROM #{quote_if_needed(user_id)}"
      else
        "REVOKE #{perms_str} ON #{event_types_str} FROM #{quote_if_needed(user_id)}"
      end
      execute(command)
    end

    # Revoke permissions from a user for event types (raising)
    #
    # @param event_types [Array<String>] Array of event type names
    # @param user_id [String] The user ID to revoke permissions from
    # @param permissions [Array<String>, nil] Optional array of permissions to revoke. If nil, revokes all permissions.
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def revoke_permission!(event_types:, user_id:, permissions: nil)
      perms_str = if permissions && !permissions.empty?
        permissions.map(&:upcase).join(",")
      else
        ""
      end
      event_types_str = event_types.map { |et| quote_if_needed(et) }.join(",")
      command = if perms_str.empty?
        "REVOKE ON #{event_types_str} FROM #{quote_if_needed(user_id)}"
      else
        "REVOKE #{perms_str} ON #{event_types_str} FROM #{quote_if_needed(user_id)}"
      end
      execute!(command)
    end

    # Show permissions for a user (non-raising)
    #
    # @param user_id [String] The user ID
    # @return [Hash] Result hash with :success, :data, :error
    def show_permissions(user_id:)
      command = "SHOW PERMISSIONS FOR #{quote_if_needed(user_id)}"
      execute(command)
    end

    # Show permissions for a user (raising)
    #
    # @param user_id [String] The user ID
    # @return [Array<Hash>] The response as an array of hashes
    # @raise [SnelDB::Error] (see #execute! for error types)
    def show_permissions!(user_id:)
      command = "SHOW PERMISSIONS FOR #{quote_if_needed(user_id)}"
      execute!(command)
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
    # @param response [String, Net::HTTPResponse] Response body (String for TCP) or HTTP response object
    def parse_and_normalize_response(response)
      # Handle both String (TCP) and HTTP response objects
      if response.is_a?(Net::HTTPResponse)
      content_type = response["Content-Type"] || ""
      body = response.body
      else
        # TCP response is just a string
        content_type = ""
        body = response
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
        rescue => e
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

    # Create transport based on protocol
    def create_transport
      case @protocol
      when "tcp"
        # Parse host:port format
        host, port = parse_tcp_address(@address)
        TcpTransport.new(host: host, port: port, user_id: @user_id, secret_key: @secret_key)
      when "http", "https"
        HttpTransport.new(base_url: @address, user_id: @user_id, secret_key: @secret_key)
      else
        raise ArgumentError, "Unsupported protocol: #{@protocol}. Use 'tcp' or 'http'"
      end
    end

    # Parse TCP address (host:port format)
    def parse_tcp_address(address)
      if address.include?(":")
        parts = address.split(":", 2)
        host = parts[0]
        port = parts[1].to_i
        if port == 0
          raise ArgumentError, "Invalid TCP address format: #{address}. Expected 'host:port'"
        end
        [host, port]
      else
        raise ArgumentError, "Invalid TCP address format: #{address}. Expected 'host:port'"
      end
    end

    # Quote a value if it contains special characters or spaces
    def quote_if_needed(value)
      case value
      when String
        # If it's already quoted, return as-is
        if value.match?(/^"[^"]*"$/)
          value
        # If it starts with a digit, always quote (could be parsed as number)
        elsif value.match?(/^\d/)
          "\"#{value}\""
        # If it's a simple word (letters, numbers, underscore, hyphen), return as-is
        elsif value.match?(/^[a-zA-Z0-9_-]+$/)
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

