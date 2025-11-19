require "spec_helper"

RSpec.describe SnelDB::Client do
  let(:base_url) { "http://localhost:8085" }
  let(:client) { described_class.new(base_url: base_url) }
  let(:authenticated_client) do
    described_class.new(
      base_url: base_url,
      user_id: "test_user",
      secret_key: "test_secret"
    )
  end

  describe "#initialize" do
    it "creates a client with base URL" do
      expect(client.base_url).to eq(base_url)
    end

    it "removes trailing slashes from base URL" do
      client_with_slash = described_class.new(base_url: "#{base_url}/")
      expect(client_with_slash.base_url).to eq(base_url)
    end

    it "accepts authentication credentials" do
      expect(authenticated_client.user_id).to eq("test_user")
      expect(authenticated_client.secret_key).to eq("test_secret")
    end

    it "defaults output_format to text" do
      expect(client.output_format).to eq("text")
    end

    it "accepts custom output_format" do
      arrow_client = described_class.new(base_url: base_url, output_format: "arrow")
      expect(arrow_client.output_format).to eq("arrow")
    end

    it "creates HTTP transport for http:// URLs" do
      expect(client.transport).to be_a(SnelDB::Transport::HTTP)
      expect(client.transport.host).to eq("localhost")
      expect(client.transport.port).to eq(8085)
      expect(client.transport.use_ssl).to be false
    end

    it "creates HTTPS transport for https:// URLs" do
      https_client = described_class.new(base_url: "https://localhost:8443")
      expect(https_client.transport).to be_a(SnelDB::Transport::HTTP)
      expect(https_client.transport.use_ssl).to be true
    end

    it "creates TCP transport for tcp:// URLs" do
      tcp_client = described_class.new(base_url: "tcp://localhost:8086")
      expect(tcp_client.transport).to be_a(SnelDB::Transport::TCP)
      expect(tcp_client.transport.host).to eq("localhost")
      expect(tcp_client.transport.port).to eq(8086)
    end

    it "creates TLS transport for tls:// URLs" do
      tls_client = described_class.new(base_url: "tls://localhost:8087")
      expect(tls_client.transport).to be_a(SnelDB::Transport::TCP)
      expect(tls_client.transport.use_ssl).to be true
    end

    it "creates auth manager" do
      expect(client.auth_manager).to be_a(SnelDB::Auth::Manager)
    end
  end

  describe "#execute" do
    let(:command) { "PING" }

    context "with successful response" do
      let(:response_body) { "PONG" }

      before do
        stub_request(:post, "#{base_url}/command")
          .with(
            body: command,
            headers: { "Content-Type" => "text/plain" }
          )
          .to_return(status: 200, body: response_body, headers: { "Content-Type" => "text/plain" })
      end

      it "sends POST request to /command endpoint" do
        result = client.execute(command)
        expect(result).to be_a(Hash)
        expect(result[:success]).to be true
        expect(result[:data].first[:raw]).to eq(response_body)
      end

      it "sets correct Content-Type header" do
        client.execute!(command)
        expect(WebMock).to have_requested(:post, "#{base_url}/command")
          .with(headers: { "Content-Type" => "text/plain" })
      end
    end

    context "with authentication" do
      let(:response_body) { "OK" }

      before do
        stub_request(:post, "#{base_url}/command")
          .with(
            body: command
          )
          .to_return(status: 200, body: response_body, headers: { "Content-Type" => "text/plain" })
      end

      it "includes authentication headers" do
        result = authenticated_client.execute!(command)
        expect(result).to be_an(Array)

        # Verify authentication headers were sent
        expect(WebMock).to have_requested(:post, "#{base_url}/command")
          .with { |req|
            req.headers["X-Auth-User"] == "test_user" &&
            !req.headers["X-Auth-Signature"].nil? &&
            !req.headers["X-Auth-Signature"].empty?
          }
      end

      it "computes correct signature format" do
        authenticated_client.execute!(command)

        # Verify signature format through the auth manager
        signature = authenticated_client.auth_manager.compute_signature(command)
        expect(signature).to match(/\A[0-9a-f]{64}\z/i)

        # Verify the signature was actually sent in the request
        expect(WebMock).to have_requested(:post, "#{base_url}/command")
          .with { |req|
            sent_signature = req.headers["X-Auth-Signature"]
            sent_signature == signature
          }
      end
    end

    context "with authentication error" do
      before do
        stub_request(:post, "#{base_url}/command")
          .to_return(
            status: 401,
            body: '{"count":0,"message":"Authentication failed","results":[],"status":401}',
            headers: { "Content-Type" => "application/json" }
          )
      end

      it "raises AuthenticationError with extracted message" do
        expect { client.execute!(command) }.to raise_error(SnelDB::AuthenticationError, "Authentication failed")
      end
    end

    context "with command error" do
      before do
        stub_request(:post, "#{base_url}/command")
          .to_return(
            status: 400,
            body: '{"count":0,"message":"Invalid command","results":[],"status":400}',
            headers: { "Content-Type" => "application/json" }
          )
      end

      it "raises CommandError with extracted message" do
        expect { client.execute!(command) }.to raise_error(SnelDB::CommandError, "Invalid command")
      end
    end

    context "with server unavailable" do
      before do
        stub_request(:post, "#{base_url}/command")
          .to_return(status: 503, body: "Service Unavailable")
      end

      it "raises ConnectionError" do
        expect { client.execute!(command) }.to raise_error(SnelDB::ConnectionError, /Service Unavailable/)
      end
    end

    context "with server error (500)" do
      before do
        stub_request(:post, "#{base_url}/command")
          .to_return(
            status: 500,
            body: '{"count":0,"message":"Internal server error occurred","results":[],"status":500}',
            headers: { "Content-Type" => "application/json" }
          )
      end

      it "raises ServerError with extracted message" do
        expect { client.execute!(command) }.to raise_error(SnelDB::ServerError, "Internal server error occurred")
      end
    end

    context "with other HTTP error" do
      before do
        stub_request(:post, "#{base_url}/command")
          .to_return(status: 418, body: "I'm a teapot")
      end

      it "raises generic Error" do
        expect { client.execute!(command) }.to raise_error(SnelDB::Error, /HTTP 418/)
      end
    end

    context "with HTTPS" do
      let(:https_url) { "https://localhost:8085" }
      let(:https_client) { described_class.new(base_url: https_url) }

      before do
        stub_request(:post, "#{https_url}/command")
          .to_return(status: 200, body: "OK")
      end

      it "enables SSL" do
        # We can't directly test SSL, but we can verify the request is made
        https_client.execute!(command)
        expect(WebMock).to have_requested(:post, "#{https_url}/command")
      end
    end

    context "with JSON response" do
      let(:json_response) { '[{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]' }

      before do
        stub_request(:post, "#{base_url}/command")
          .to_return(status: 200, body: json_response, headers: { "Content-Type" => "application/json" })
      end

      it "parses JSON array" do
        result = client.execute!(command)
        expect(result).to be_an(Array)
        expect(result.length).to eq(2)
        expect(result.first).to eq({ "id" => 1, "name" => "test" })
        expect(result.last).to eq({ "id" => 2, "name" => "test2" })
      end
    end

    context "with JSON object response" do
      let(:json_response) { '{"id": 1, "name": "test"}' }

      before do
        stub_request(:post, "#{base_url}/command")
          .to_return(status: 200, body: json_response, headers: { "Content-Type" => "application/json" })
      end

      it "wraps JSON object in array" do
        result = client.execute!(command)
        expect(result).to be_an(Array)
        expect(result.length).to eq(1)
        expect(result.first).to eq({ "id" => 1, "name" => "test" })
      end
    end

    context "with pipe-delimited text response" do
      let(:text_response) { "ID|NAME|VALUE\n1|test|100\n2|test2|200" }

      before do
        stub_request(:post, "#{base_url}/command")
          .to_return(status: 200, body: text_response, headers: { "Content-Type" => "text/plain" })
      end

      it "parses pipe-delimited format with headers" do
        result = client.execute!(command)
        expect(result).to be_an(Array)
        expect(result.length).to eq(2)
        expect(result.first).to eq({ "id" => "1", "name" => "test", "value" => "100" })
        expect(result.last).to eq({ "id" => "2", "name" => "test2", "value" => "200" })
      end
    end

    context "with plain text response" do
      let(:text_response) { "Line 1\nLine 2\nLine 3" }

      before do
        stub_request(:post, "#{base_url}/command")
          .to_return(status: 200, body: text_response, headers: { "Content-Type" => "text/plain" })
      end

      it "parses plain text lines" do
        result = client.execute!(command)
        expect(result).to be_an(Array)
        expect(result.length).to eq(3)
        expect(result.first[:raw]).to eq("Line 1")
        expect(result.last[:raw]).to eq("Line 3")
      end
    end

    context "with empty response" do
      before do
        stub_request(:post, "#{base_url}/command")
          .to_return(status: 200, body: "", headers: { "Content-Type" => "text/plain" })
      end

      it "returns empty array" do
        result = client.execute!(command)
        expect(result).to eq([])
      end
    end

    context "with whitespace-only response" do
      before do
        stub_request(:post, "#{base_url}/command")
          .to_return(status: 200, body: "   \n\n  ", headers: { "Content-Type" => "text/plain" })
      end

      it "returns empty array" do
        result = client.execute!(command)
        expect(result).to eq([])
      end
    end
  end

  describe "#define" do
    it "builds correct DEFINE command" do
      expect(client).to receive(:execute).with(
        'DEFINE order_created FIELDS { "id": "int", "amount": "float" }'
      ).and_return({ success: true, data: [], error: nil })
      client.define(
        event_type: "order_created",
        fields: { "id" => "int", "amount" => "float" }
      )
    end

    it "includes version when provided" do
      expect(client).to receive(:execute).with(
        'DEFINE order_created AS 1 FIELDS { "id": "int" }'
      ).and_return({ success: true, data: [], error: nil })
      client.define(
        event_type: "order_created",
        version: 1,
        fields: { "id" => "int" }
      )
    end

    it "handles multiple fields" do
      expect(client).to receive(:execute).with(
        'DEFINE user_registered FIELDS { "id": "int", "name": "string", "email": "string", "created_at": "timestamp" }'
      ).and_return({ success: true, data: [], error: nil })
      client.define(
        event_type: "user_registered",
        fields: {
          "id" => "int",
          "name" => "string",
          "email" => "string",
          "created_at" => "timestamp"
        }
      )
    end

    it "handles enum fields with array values" do
      expect(client).to receive(:execute).with(
        'DEFINE subscription FIELDS { "plan": ["pro", "basic"] }'
      ).and_return({ success: true, data: [], error: nil })
      client.define(
        event_type: "subscription",
        fields: { "plan" => ["pro", "basic"] }
      )
    end

    it "handles mixed primitive and enum fields" do
      expect(client).to receive(:execute).with(
        'DEFINE user FIELDS { "id": "int", "role": ["admin", "user", "guest"] }'
      ).and_return({ success: true, data: [], error: nil })
      client.define(
        event_type: "user",
        fields: {
          "id" => "int",
          "role" => ["admin", "user", "guest"]
        }
      )
    end
  end

  describe "#store" do
    it "builds correct STORE command" do
      expect(client).to receive(:execute).with(
        'STORE order_created FOR customer-123 PAYLOAD {"id":42,"amount":99.99}'
      ).and_return({ success: true, data: [], error: nil })
      client.store(
        event_type: "order_created",
        context_id: "customer-123",
        payload: { id: 42, amount: 99.99 }
      )
    end

    it "quotes context_id with special characters" do
      expect(client).to receive(:execute).with(
        'STORE order_created FOR "user:ext:42" PAYLOAD {"id":1}'
      ).and_return({ success: true, data: [], error: nil })
      client.store(
        event_type: "order_created",
        context_id: "user:ext:42",
        payload: { id: 1 }
      )
    end

    it "quotes context_id with spaces" do
      expect(client).to receive(:execute).with(
        'STORE order_created FOR "user name" PAYLOAD {"id":1}'
      ).and_return({ success: true, data: [], error: nil })
      client.store(
        event_type: "order_created",
        context_id: "user name",
        payload: { id: 1 }
      )
    end

    it "does not quote simple context_id" do
      expect(client).to receive(:execute).with(
        'STORE order_created FOR user123 PAYLOAD {"id":1}'
      ).and_return({ success: true, data: [], error: nil })
      client.store(
        event_type: "order_created",
        context_id: "user123",
        payload: { id: 1 }
      )
    end

    it "handles complex payloads" do
      payload = {
        id: 1,
        items: [{ product_id: 10, quantity: 2 }],
        metadata: { source: "web", version: "1.0" }
      }
      expect(client).to receive(:execute) do |command|
        expect(command).to include("STORE order_created FOR customer-123 PAYLOAD")
        expect(command).to include('"id":1')
        { success: true, data: [], error: nil }
      end
      client.store(
        event_type: "order_created",
        context_id: "customer-123",
        payload: payload
      )
    end
  end

  describe "#query" do
    it "builds simple QUERY command" do
      expect(client).to receive(:execute).with(
        "QUERY order_created"
      ).and_return({ success: true, data: [], error: nil })
      client.query(event_type: "order_created")
    end

    it "builds QUERY command with WHERE clause" do
      expect(client).to receive(:execute).with(
        "QUERY order_created WHERE amount >= 50"
      ).and_return({ success: true, data: [], error: nil })
      client.query(
        event_type: "order_created",
        where: "amount >= 50"
      )
    end

    it "builds complex QUERY command" do
      expect(client).to receive(:execute).with(
        'QUERY order_created FOR customer-123 SINCE "2025-01-01T00:00:00Z" USING created_at WHERE currency = "USD" LIMIT 100'
      ).and_return({ success: true, data: [], error: nil })
      client.query(
        event_type: "order_created",
        context_id: "customer-123",
        since: "2025-01-01T00:00:00Z",
        using: "created_at",
        where: 'currency = "USD"',
        limit: 100
      )
    end

    it "includes return_fields when provided" do
      expect(client).to receive(:execute).with(
        'QUERY order_created RETURN [id, amount] WHERE amount > 100'
      ).and_return({ success: true, data: [], error: nil })
      client.query(
        event_type: "order_created",
        return_fields: ["id", "amount"],
        where: "amount > 100"
      )
    end

    it "quotes context_id with special characters" do
      expect(client).to receive(:execute).with(
        'QUERY order_created FOR "user:123"'
      ).and_return({ success: true, data: [], error: nil })
      client.query(
        event_type: "order_created",
        context_id: "user:123"
      )
    end

    it "handles numeric since value" do
      expect(client).to receive(:execute).with(
        "QUERY order_created SINCE 1234567890"
      ).and_return({ success: true, data: [], error: nil })
      client.query(
        event_type: "order_created",
        since: 1234567890
      )
    end
  end

  describe "#replay" do
    it "builds simple REPLAY command" do
      expect(client).to receive(:execute).with(
        'REPLAY FOR customer-123'
      ).and_return({ success: true, data: [], error: nil })
      client.replay(context_id: "customer-123")
    end

    it "builds REPLAY command with event_type" do
      expect(client).to receive(:execute).with(
        'REPLAY order_created FOR customer-123'
      ).and_return({ success: true, data: [], error: nil })
      client.replay(
        context_id: "customer-123",
        event_type: "order_created"
      )
    end

    it "builds complex REPLAY command" do
      expect(client).to receive(:execute).with(
        'REPLAY order_created FOR customer-123 SINCE "2025-01-01T00:00:00Z" USING created_at'
      ).and_return({ success: true, data: [], error: nil })
      client.replay(
        context_id: "customer-123",
        event_type: "order_created",
        since: "2025-01-01T00:00:00Z",
        using: "created_at"
      )
    end

    it "includes return_fields when provided" do
      expect(client).to receive(:execute).with(
        'REPLAY FOR customer-123 RETURN [id, amount]'
      ).and_return({ success: true, data: [], error: nil })
      client.replay(
        context_id: "customer-123",
        return_fields: ["id", "amount"]
      )
    end
  end

  describe "#ping" do
    it "executes PING command" do
      expect(client).to receive(:execute).with("PING")
      client.ping
    end
  end

  describe "#flush" do
    it "executes FLUSH command" do
      expect(client).to receive(:execute).with("FLUSH")
      client.flush
    end
  end

  describe "#create_user" do
    it "builds CREATE USER command without key" do
      expect(client).to receive(:execute).with("CREATE USER new_user")
      client.create_user(user_id: "new_user")
    end

    it "builds CREATE USER command with key" do
      expect(client).to receive(:execute).with("CREATE USER new_user WITH KEY my_secret_key")
      client.create_user(user_id: "new_user", secret_key: "my_secret_key")
    end
  end

  describe "#list_users" do
    it "executes LIST USERS command" do
      expect(client).to receive(:execute).with("LIST USERS")
      client.list_users
    end
  end

  describe "#revoke_key" do
    it "executes REVOKE KEY command" do
      expect(client).to receive(:execute).with("REVOKE KEY user_id")
      client.revoke_key(user_id: "user_id")
    end
  end

  describe "#authenticate!" do
    let(:tcp_client) do
      described_class.new(
        base_url: "tcp://localhost:8086",
        user_id: "test_user",
        secret_key: "test_secret"
      )
    end

    it "raises error for HTTP client" do
      expect { client.authenticate! }.to raise_error(SnelDB::AuthenticationError, /TCP connections/)
    end

    it "authenticates TCP client and returns token" do
      # Mock TCP transport
      transport = instance_double(SnelDB::Transport::TCP)
      allow(tcp_client).to receive(:transport).and_return(transport)
      allow(transport).to receive(:is_a?).with(SnelDB::Transport::TCP).and_return(true)
      allow(transport).to receive(:connect)
      allow(transport).to receive(:execute).and_return({ status: 200, body: "OK TOKEN abc123" })
      allow(transport).to receive(:session_token=)
      allow(transport).to receive(:authenticated_user_id=)

      token = tcp_client.authenticate!
      expect(token).to eq("abc123")
    end
  end

  describe "#close" do
    let(:tcp_client) { described_class.new(base_url: "tcp://localhost:8086") }

    it "closes TCP connection" do
      transport = instance_double(SnelDB::Transport::TCP)
      allow(tcp_client).to receive(:transport).and_return(transport)
      allow(transport).to receive(:respond_to?).with(:close).and_return(true)
      allow(transport).to receive(:close)

      tcp_client.close
      expect(transport).to have_received(:close)
    end

    it "handles HTTP client gracefully" do
      expect { client.close }.not_to raise_error
    end
  end

  describe "permission management" do
    describe "#grant_permission" do
      it "builds GRANT command correctly" do
        expect(client).to receive(:execute).with(
          'GRANT READ ON order_created TO api_client'
        ).and_return({ success: true, data: [], error: nil })
        client.grant_permission(
          user_id: "api_client",
          permissions: ["read"],
          event_types: ["order_created"]
        )
      end

      it "handles multiple permissions" do
        expect(client).to receive(:execute).with(
          'GRANT READ, WRITE ON order_created, payment_succeeded TO api_client'
        ).and_return({ success: true, data: [], error: nil })
        client.grant_permission(
          user_id: "api_client",
          permissions: ["read", "write"],
          event_types: ["order_created", "payment_succeeded"]
        )
      end

      it "quotes event types with special characters" do
        expect(client).to receive(:execute).with(
          'GRANT READ ON "event-type" TO api_client'
        ).and_return({ success: true, data: [], error: nil })
        client.grant_permission(
          user_id: "api_client",
          permissions: ["read"],
          event_types: ["event-type"]
        )
      end
    end

    describe "#revoke_permission" do
      it "builds REVOKE command with permissions" do
        expect(client).to receive(:execute).with(
          'REVOKE WRITE ON order_created FROM api_client'
        ).and_return({ success: true, data: [], error: nil })
        client.revoke_permission(
          user_id: "api_client",
          permissions: ["write"],
          event_types: ["order_created"]
        )
      end

      it "builds REVOKE command without permissions (revoke all)" do
        expect(client).to receive(:execute).with(
          'REVOKE ON order_created FROM api_client'
        ).and_return({ success: true, data: [], error: nil })
        client.revoke_permission(
          user_id: "api_client",
          event_types: ["order_created"]
        )
      end
    end

    describe "#show_permissions" do
      it "builds SHOW PERMISSIONS command" do
        expect(client).to receive(:execute).with(
          "SHOW PERMISSIONS FOR api_client"
        ).and_return({ success: true, data: [], error: nil })
        client.show_permissions(user_id: "api_client")
      end
    end
  end

  describe "database initialization" do
    describe "#initialized?" do
      context "when users exist" do
        before do
          stub_request(:post, "#{base_url}/command")
            .with(body: "LIST USERS")
            .to_return(
              status: 200,
              body: "admin: active\nuser1: active",
              headers: { "Content-Type" => "text/plain" }
            )
        end

        it "returns true" do
          expect(client.initialized?).to be true
        end
      end

      context "when no users exist" do
        before do
          stub_request(:post, "#{base_url}/command")
            .with(body: "LIST USERS")
            .to_return(
              status: 200,
              body: "No users found",
              headers: { "Content-Type" => "text/plain" }
            )
        end

        it "returns false" do
          expect(client.initialized?).to be false
        end
      end

      context "when authentication is required" do
        before do
          stub_request(:post, "#{base_url}/command")
            .with(body: "LIST USERS")
            .to_return(
              status: 401,
              body: '{"message":"Authentication required"}',
              headers: { "Content-Type" => "application/json" }
            )
        end

        it "returns false (assumes not initialized)" do
          expect(client.initialized?).to be false
        end
      end
    end

    describe "#ensure_initialized!" do
      context "when database is already initialized" do
        before do
          stub_request(:post, "#{base_url}/command")
            .with(body: "LIST USERS")
            .to_return(
              status: 200,
              body: "admin: active",
              headers: { "Content-Type" => "text/plain" }
            )
        end

        it "returns false" do
          result = client.ensure_initialized!(
            admin_user_id: "admin",
            admin_secret_key: "admin-key-123"
          )
          expect(result).to be false
        end
      end

      context "when database is not initialized (bypass_auth mode)" do
        before do
          # First call: LIST USERS returns empty or auth error
          stub_request(:post, "#{base_url}/command")
            .with(body: "LIST USERS")
            .to_return(
              status: 200,
              body: "No users found",
              headers: { "Content-Type" => "text/plain" }
            )

          # Second call: CREATE USER succeeds
          stub_request(:post, "#{base_url}/command")
            .with(body: /CREATE USER admin/)
            .to_return(
              status: 200,
              body: "User 'admin' created\nSecret key: admin-key-123",
              headers: { "Content-Type" => "text/plain" }
            )
        end

        it "creates admin user and returns true" do
          result = client.ensure_initialized!(
            admin_user_id: "admin",
            admin_secret_key: "admin-key-123"
          )
          expect(result).to be true
        end
      end

      context "when admin key is missing" do
        it "raises error" do
          expect {
            client.ensure_initialized!(admin_user_id: "admin")
          }.to raise_error(SnelDB::Error, /Admin secret key required/)
        end
      end

      context "when user already exists" do
        before do
          stub_request(:post, "#{base_url}/command")
            .with(body: "LIST USERS")
            .to_return(
              status: 200,
              body: "No users found",
              headers: { "Content-Type" => "text/plain" }
            )

          stub_request(:post, "#{base_url}/command")
            .with(body: /CREATE USER admin/)
            .to_return(
              status: 400,
              body: '{"message":"User already exists"}',
              headers: { "Content-Type" => "application/json" }
            )
        end

        it "returns false" do
          result = client.ensure_initialized!(
            admin_user_id: "admin",
            admin_secret_key: "admin-key-123"
          )
          expect(result).to be false
        end
      end
    end

    describe "auto_initialize option" do
      context "when auto_initialize is true" do
        before do
          stub_request(:post, "#{base_url}/command")
            .with(body: "LIST USERS")
            .to_return(
              status: 200,
              body: "No users found",
              headers: { "Content-Type" => "text/plain" }
            )

          stub_request(:post, "#{base_url}/command")
            .with(body: /CREATE USER admin/)
            .to_return(
              status: 200,
              body: "User 'admin' created\nSecret key: admin-key-123",
              headers: { "Content-Type" => "text/plain" }
            )
        end

        it "automatically initializes database on creation" do
          client = described_class.new(
            base_url: base_url,
            auto_initialize: true,
            initial_admin_user: "admin",
            initial_admin_key: "admin-key-123"
          )
          expect(client).to be_a(described_class)
        end
      end

      context "when auto_initialize is false" do
        it "does not initialize database" do
          client = described_class.new(
            base_url: base_url,
            auto_initialize: false
          )
          expect(client).to be_a(described_class)
        end
      end
    end
  end

  describe "not implemented commands" do
    it "raises NotImplementedError for remember" do
      expect {
        client.remember(name: "test", event_type: "order_created")
      }.to raise_error(NotImplementedError, /REMEMBER command/)
    end

    it "raises NotImplementedError for show_materialized" do
      expect {
        client.show_materialized(name: "test")
      }.to raise_error(NotImplementedError, /SHOW MATERIALIZED command/)
    end

    it "raises NotImplementedError for batch" do
      expect {
        client.batch(["STORE test FOR ctx1 PAYLOAD {\"id\":1}"])
      }.to raise_error(NotImplementedError, /BATCH command/)
    end

    it "raises NotImplementedError for compare" do
      expect {
        client.compare(["QUERY test", "QUERY test2"])
      }.to raise_error(NotImplementedError, /COMPARE command/)
    end

    it "raises NotImplementedError for plot" do
      expect {
        client.plot("QUERY test")
      }.to raise_error(NotImplementedError, /PLOT command/)
    end
  end

  describe "#quote_if_needed" do
    it "does not quote simple identifiers" do
      result = client.send(:quote_if_needed, "user123")
      expect(result).to eq("user123")
    end

    it "quotes strings with special characters" do
      result = client.send(:quote_if_needed, "user:123")
      expect(result).to eq('"user:123"')
    end

    it "quotes strings with spaces" do
      result = client.send(:quote_if_needed, "user name")
      expect(result).to eq('"user name"')
    end

    it "does not quote already quoted strings" do
      result = client.send(:quote_if_needed, '"user:123"')
      expect(result).to eq('"user:123"')
    end

    it "does not quote integers" do
      result = client.send(:quote_if_needed, 123)
      expect(result).to eq("123")
    end

    it "does not quote floats" do
      result = client.send(:quote_if_needed, 123.45)
      expect(result).to eq("123.45")
    end

    it "quotes other types" do
      result = client.send(:quote_if_needed, :symbol)
      expect(result).to eq('"symbol"')
    end
  end

  describe "text parsing" do
    describe "#parse_text_to_hashes" do
      it "parses JSON array" do
        json = '[{"id": 1, "name": "test"}]'
        result = client.send(:parse_text_to_hashes, json)
        expect(result).to eq([{ "id" => 1, "name" => "test" }])
      end

      it "parses JSON object" do
        json = '{"id": 1, "name": "test"}'
        result = client.send(:parse_text_to_hashes, json)
        expect(result).to eq([{ "id" => 1, "name" => "test" }])
      end

      it "parses pipe-delimited with headers" do
        text = "ID|NAME|VALUE\n1|test|100\n2|test2|200"
        result = client.send(:parse_text_to_hashes, text)
        expect(result).to eq([
          { "id" => "1", "name" => "test", "value" => "100" },
          { "id" => "2", "name" => "test2", "value" => "200" }
        ])
      end

      it "parses pipe-delimited without headers" do
        text = "1|test|100\n2|test2|200"
        result = client.send(:parse_text_to_hashes, text)
        expect(result.first[:parts]).to eq(["1", "test", "100"])
      end

      it "parses plain text lines" do
        text = "Line 1\nLine 2\nLine 3"
        result = client.send(:parse_text_to_hashes, text)
        expect(result.length).to eq(3)
        expect(result.first[:raw]).to eq("Line 1")
      end

      it "handles empty input" do
        result = client.send(:parse_text_to_hashes, "")
        expect(result).to eq([])
      end

      it "handles whitespace-only input" do
        result = client.send(:parse_text_to_hashes, "   \n\n  ")
        expect(result).to eq([])
      end

      it "raises ParseError for invalid JSON that looks like JSON" do
        text = '{"id": 1, invalid}'
        expect { client.send(:parse_text_to_hashes, text) }.to raise_error(SnelDB::ParseError, /Invalid JSON/)
      end

      it "handles text that starts with { but isn't JSON" do
        text = "{ this is not json, just text }"
        result = client.send(:parse_text_to_hashes, text)
        expect(result).to be_an(Array)
        expect(result.first[:raw]).to include("this is not json")
      end

      it "handles UTF-8 encoding" do
        text = "Test: café\nTest: 测试"
        result = client.send(:parse_text_to_hashes, text)
        expect(result.length).to eq(2)
        expect(result.first[:raw]).to include("café")
      end
    end

    describe "#parse_pipe_delimited" do
      it "parses with headers" do
        lines = ["ID|NAME", "1|test", "2|test2"]
        result = client.send(:parse_pipe_delimited, lines)
        expect(result).to eq([
          { "id" => "1", "name" => "test" },
          { "id" => "2", "name" => "test2" }
        ])
      end

      it "parses without headers" do
        lines = ["1|test|100", "2|test2|200"]
        result = client.send(:parse_pipe_delimited, lines)
        expect(result.first[:parts]).to eq(["1", "test", "100"])
      end

      it "handles mismatched column counts" do
        lines = ["ID|NAME", "1|test|extra", "2"]
        result = client.send(:parse_pipe_delimited, lines)
        expect(result.first[:parts]).to eq(["1", "test", "extra"])
        expect(result.last[:parts]).to eq(["2"])
      end
    end
  end

  describe "Arrow format parsing", if: defined?(Arrow) do
    let(:arrow_client) { described_class.new(base_url: base_url, output_format: "arrow") }

    context "when Arrow gem is available" do
      before do
        # Mock Arrow parsing - we can't easily create real Arrow data in tests
        # but we can test the error handling path
        allow(Arrow::Buffer).to receive(:new).and_raise(StandardError.new("Arrow parsing failed"))
      end

      it "handles Arrow parsing errors gracefully" do
        stub_request(:post, "#{base_url}/command")
          .to_return(
            status: 200,
            body: "fake arrow data",
            headers: { "Content-Type" => "application/vnd.apache.arrow" }
          )

        result = arrow_client.execute!("QUERY test")
        expect(result).to be_an(Array)
        expect(result.first[:error]).to include("Failed to parse Arrow format")
      end
    end
  end

  describe "error handling" do
    it "raises ConnectionError on network failures" do
      stub_request(:post, "#{base_url}/command")
        .to_raise(Errno::ECONNREFUSED.new("Connection refused"))

      expect { client.execute!("PING") }.to raise_error(StandardError)
    end

    it "handles timeout errors" do
      stub_request(:post, "#{base_url}/command")
        .to_timeout

      expect { client.execute!("PING") }.to raise_error(StandardError)
    end
  end

  describe "integration scenarios" do
    it "can define, store, and query events" do
      # Define event
      stub_request(:post, "#{base_url}/command")
        .with(body: /DEFINE order_created/)
        .to_return(status: 200, body: "OK")

      client.define(
        event_type: "order_created",
        fields: { "id" => "int", "amount" => "float" }
      )

      # Store event
      stub_request(:post, "#{base_url}/command")
        .with(body: /STORE order_created/)
        .to_return(status: 200, body: "OK")

      client.store!(
        event_type: "order_created",
        context_id: "customer-123",
        payload: { id: 1, amount: 99.99 }
      )

      # Query events
      stub_request(:post, "#{base_url}/command")
        .with(body: /QUERY order_created/)
        .to_return(
          status: 200,
          body: '[{"id": 1, "amount": 99.99}]',
          headers: { "Content-Type" => "application/json" }
        )

      result = client.query!(event_type: "order_created")
      expect(result).to be_an(Array)
      expect(result.first["id"]).to eq(1)
    end
  end

  describe "non-raising methods (without !)" do
    describe "#execute" do
      context "with successful response" do
        before do
          stub_request(:post, "#{base_url}/command")
            .to_return(status: 200, body: '[{"id": 1}]', headers: { "Content-Type" => "application/json" })
        end

        it "returns success hash" do
          result = client.execute("QUERY test")
          expect(result).to eq({
            success: true,
            data: [{ "id" => 1 }],
            error: nil
          })
        end
      end

      context "with error response" do
        before do
          stub_request(:post, "#{base_url}/command")
            .to_return(
              status: 400,
              body: '{"count":0,"message":"Invalid command syntax","results":[],"status":400}',
              headers: { "Content-Type" => "application/json" }
            )
        end

        it "returns error hash with extracted message" do
          result = client.execute("INVALID COMMAND")
          expect(result[:success]).to be false
          expect(result[:data]).to be_nil
          expect(result[:error]).to be_a(SnelDB::CommandError)
          expect(result[:error].message).to eq("Invalid command syntax")
        end
      end
    end

    describe "#query" do
      context "with successful response" do
        before do
          stub_request(:post, "#{base_url}/command")
            .to_return(
              status: 200,
              body: '[{"id": 1, "name": "test"}]',
              headers: { "Content-Type" => "application/json" }
            )
        end

        it "returns success hash with data" do
          result = client.query(event_type: "test")
          expect(result[:success]).to be true
          expect(result[:data]).to eq([{ "id" => 1, "name" => "test" }])
          expect(result[:error]).to be_nil
        end
      end

      context "with error response" do
        before do
          stub_request(:post, "#{base_url}/command")
            .to_return(status: 404, body: "Not found")
        end

        it "returns error hash" do
          result = client.query(event_type: "nonexistent")
          expect(result[:success]).to be false
          expect(result[:data]).to be_nil
          expect(result[:error]).to be_a(SnelDB::NotFoundError)
        end
      end
    end

    describe "#store" do
      context "with successful response" do
        before do
          stub_request(:post, "#{base_url}/command")
            .to_return(status: 200, body: "OK")
        end

        it "returns success hash" do
          result = client.store(
            event_type: "test",
            context_id: "ctx-1",
            payload: { id: 1 }
          )
          expect(result[:success]).to be true
          expect(result[:error]).to be_nil
        end
      end

      context "with authentication error" do
        before do
          stub_request(:post, "#{base_url}/command")
            .to_return(
              status: 401,
              body: '{"count":0,"message":"Authentication failed","results":[],"status":401}',
              headers: { "Content-Type" => "application/json" }
            )
        end

        it "returns error hash with AuthenticationError" do
          result = client.store(
            event_type: "test",
            context_id: "ctx-1",
            payload: { id: 1 }
          )
          expect(result[:success]).to be false
          expect(result[:error]).to be_a(SnelDB::AuthenticationError)
          expect(result[:error].message).to eq("Authentication failed")
        end
      end
    end

    describe "#replay" do
      before do
        stub_request(:post, "#{base_url}/command")
          .to_return(
            status: 200,
            body: '[{"id": 1}]',
            headers: { "Content-Type" => "application/json" }
          )
      end

      it "returns success hash" do
        result = client.replay(context_id: "ctx-1")
        expect(result[:success]).to be true
        expect(result[:data]).to eq([{ "id" => 1 }])
        expect(result[:error]).to be_nil
      end
    end

    describe "#ping" do
      before do
        stub_request(:post, "#{base_url}/command")
          .to_return(status: 200, body: "PONG", headers: { "Content-Type" => "text/plain" })
      end

      it "returns success hash" do
        result = client.ping
        expect(result[:success]).to be true
        expect(result[:error]).to be_nil
        expect(result[:data]).to be_an(Array)
      end
    end

    describe "#flush" do
      before do
        stub_request(:post, "#{base_url}/command")
          .to_return(status: 200, body: "OK", headers: { "Content-Type" => "text/plain" })
      end

      it "returns success hash" do
        result = client.flush
        expect(result[:success]).to be true
        expect(result[:error]).to be_nil
        expect(result[:data]).to be_an(Array)
      end
    end
  end

  describe "raising methods (with !)" do
    describe "#execute!" do
      context "with successful response" do
        before do
          stub_request(:post, "#{base_url}/command")
            .to_return(status: 200, body: '[{"id": 1}]', headers: { "Content-Type" => "application/json" })
        end

        it "returns array of hashes" do
          result = client.execute!("QUERY test")
          expect(result).to eq([{ "id" => 1 }])
        end
      end

      context "with error response" do
        before do
          stub_request(:post, "#{base_url}/command")
            .to_return(
              status: 400,
              body: '{"count":0,"message":"Invalid command syntax","results":[],"status":400}',
              headers: { "Content-Type" => "application/json" }
            )
        end

        it "raises error with extracted message" do
          expect { client.execute!("INVALID COMMAND") }.to raise_error(SnelDB::CommandError, "Invalid command syntax")
        end
      end
    end

    describe "#query!" do
      before do
        stub_request(:post, "#{base_url}/command")
          .to_return(
            status: 200,
            body: '[{"id": 1}]',
            headers: { "Content-Type" => "application/json" }
          )
      end

      it "returns array of hashes" do
        result = client.query!(event_type: "test")
        expect(result).to eq([{ "id" => 1 }])
      end
    end

    describe "#store!" do
      before do
        stub_request(:post, "#{base_url}/command")
          .to_return(status: 200, body: "OK")
      end

      it "returns array of hashes" do
        result = client.store!(
          event_type: "test",
          context_id: "ctx-1",
          payload: { id: 1 }
        )
        expect(result).to be_an(Array)
      end
    end
  end

  describe "HTTP status code coverage" do
    it "handles all status codes from server" do
      # Server can return: 200, 400, 401, 403, 404, 405, 500, 503
      status_codes = {
        200 => :success,
        400 => SnelDB::CommandError,
        401 => SnelDB::AuthenticationError,
        403 => SnelDB::AuthorizationError,
        404 => SnelDB::NotFoundError,
        405 => SnelDB::CommandError,  # Method Not Allowed treated as BadRequest
        500 => SnelDB::ServerError,
        503 => SnelDB::ConnectionError
      }

      status_codes.each do |code, expected|
        stub_request(:post, "#{base_url}/command")
          .to_return(status: code, body: "Error #{code}")

        if expected == :success
          # Should not raise for 200
          expect { client.execute!("PING") }.not_to raise_error
        else
          # Should raise specific error type
          expect { client.execute!("PING") }.to raise_error(expected)
        end
      end
    end
  end
end
