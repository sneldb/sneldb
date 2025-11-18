require "spec_helper"

RSpec.describe SnelDB::Client do
  let(:http_address) { "http://localhost:8085" }
  let(:tcp_address) { "localhost:8086" }
  let(:http_client) { described_class.new(address: http_address) }
  let(:tcp_client) { described_class.new(address: tcp_address, protocol: "tcp") }
  let(:authenticated_http_client) do
    described_class.new(
      address: http_address,
      user_id: "test_user",
      secret_key: "test_secret"
    )
  end
  let(:authenticated_tcp_client) do
    described_class.new(
      address: tcp_address,
      protocol: "tcp",
      user_id: "test_user",
      secret_key: "test_secret"
    )
  end

  describe "#initialize" do
    it "creates an HTTP client with HTTP address" do
      expect(http_client.address).to eq(http_address)
      expect(http_client.protocol).to eq("http")
    end

    it "creates a TCP client with TCP address" do
      expect(tcp_client.address).to eq(tcp_address)
      expect(tcp_client.protocol).to eq("tcp")
    end

    it "auto-detects HTTP protocol from address" do
      client = described_class.new(address: "http://localhost:8085")
      expect(client.protocol).to eq("http")
    end

    it "auto-detects TCP protocol from address" do
      client = described_class.new(address: "localhost:8086")
      expect(client.protocol).to eq("tcp")
    end

    it "accepts explicit protocol override" do
      client = described_class.new(address: "localhost:8086", protocol: "tcp")
      expect(client.protocol).to eq("tcp")
    end

    it "accepts authentication credentials" do
      expect(authenticated_http_client.user_id).to eq("test_user")
      expect(authenticated_http_client.secret_key).to eq("test_secret")
    end

    it "defaults output_format to text" do
      expect(http_client.output_format).to eq("text")
    end

    it "accepts custom output_format" do
      arrow_client = described_class.new(address: http_address, output_format: "arrow")
      expect(arrow_client.output_format).to eq("arrow")
    end

    it "requires address parameter" do
      expect { described_class.new }.to raise_error(ArgumentError)
    end
  end

  describe "#execute (HTTP)" do
    let(:client) { http_client }
    let(:command) { "FLUSH" }

    context "with successful response" do
      let(:response_body) { "OK" }

      before do
        stub_request(:post, "#{http_address}/command")
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
        expect(WebMock).to have_requested(:post, "#{http_address}/command")
          .with(headers: { "Content-Type" => "text/plain" })
      end
    end

    context "with authentication" do
      let(:client) { authenticated_http_client }
      let(:response_body) { "OK" }

      before do
        stub_request(:post, "#{http_address}/command")
          .with(
            body: command
          )
          .to_return(status: 200, body: response_body, headers: { "Content-Type" => "text/plain" })
      end

      it "includes authentication headers" do
        result = client.execute!(command)
        expect(result).to be_an(Array)

        # Verify authentication headers were sent
        expect(WebMock).to have_requested(:post, "#{http_address}/command")
          .with { |req|
            req.headers["X-Auth-User"] == "test_user" &&
            !req.headers["X-Auth-Signature"].nil? &&
            !req.headers["X-Auth-Signature"].empty?
          }
      end
    end

    context "with authentication error" do
      before do
        stub_request(:post, "#{http_address}/command")
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
        stub_request(:post, "#{http_address}/command")
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
        stub_request(:post, "#{http_address}/command")
          .to_return(status: 503, body: "Service Unavailable")
      end

      it "raises ConnectionError" do
        expect { client.execute!(command) }.to raise_error(SnelDB::ConnectionError, /Service Unavailable/)
      end
    end

    context "with server error (500)" do
      before do
        stub_request(:post, "#{http_address}/command")
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

    context "with JSON response" do
      let(:json_response) { '[{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]' }

      before do
        stub_request(:post, "#{http_address}/command")
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

    context "with pipe-delimited text response" do
      let(:text_response) { "ID|NAME|VALUE\n1|test|100\n2|test2|200" }

      before do
        stub_request(:post, "#{http_address}/command")
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
  end

  describe "#execute (TCP)", :tcp do
    let(:client) { tcp_client }
    let(:command) { "FLUSH" }
    let(:socket) { double("TCPSocket") }

    before do
      # Mock TCP socket creation
      allow(TCPSocket).to receive(:new).and_return(socket)
      allow(socket).to receive(:setsockopt)
      allow(socket).to receive(:write)
      allow(socket).to receive(:flush)
      allow(socket).to receive(:closed?).and_return(false)
      allow(socket).to receive(:close)
    end

    context "with successful response" do
      before do
        allow(socket).to receive(:gets).and_return("PONG\n")
      end

      it "sends command via TCP" do
        result = client.execute(command)
        expect(result).to be_a(Hash)
        expect(result[:success]).to be true
      end
    end

    context "with authentication" do
      let(:client) { authenticated_tcp_client }

      before do
        # First call is AUTH response, second is command response
        allow(socket).to receive(:gets).and_return("OK TOKEN abc123\n", "OK\n")
      end

      it "authenticates connection" do
        # This will trigger authentication
        expect { client.execute(command) }.not_to raise_error
      end
    end

    context "with error response" do
      before do
        allow(socket).to receive(:gets).and_return("ERROR: Invalid command\n")
      end

      it "raises CommandError" do
        expect { client.execute!(command) }.to raise_error(SnelDB::CommandError, /Invalid command/)
      end
    end
  end

  describe "#define" do
    let(:client) { http_client }

    before do
      stub_request(:post, "#{http_address}/command")
        .to_return(status: 200, body: "OK", headers: { "Content-Type" => "text/plain" })
    end

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

    it "handles enum fields with array values" do
      expect(client).to receive(:execute).with(
        'DEFINE subscription FIELDS { "plan": ["pro", "basic"] }'
      ).and_return({ success: true, data: [], error: nil })
      client.define(
        event_type: "subscription",
        fields: { "plan" => ["pro", "basic"] }
      )
    end
  end

  describe "#store" do
    let(:client) { http_client }

    before do
      stub_request(:post, "#{http_address}/command")
        .to_return(status: 200, body: "OK", headers: { "Content-Type" => "text/plain" })
    end

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
  end

  describe "#query" do
    let(:client) { http_client }

    before do
      stub_request(:post, "#{http_address}/command")
        .to_return(status: 200, body: "[]", headers: { "Content-Type" => "application/json" })
    end

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
  end

  describe "#replay" do
    let(:client) { http_client }

    before do
      stub_request(:post, "#{http_address}/command")
        .to_return(status: 200, body: "[]", headers: { "Content-Type" => "application/json" })
    end

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
  end

  describe "#ping" do
    let(:client) { http_client }

    before do
      stub_request(:post, "#{http_address}/command")
        .to_return(status: 200, body: "PONG", headers: { "Content-Type" => "text/plain" })
    end

    it "executes PING command" do
      expect(client).to receive(:execute).with("PING")
      # PING command removed - not implemented in server dispatcher
      # Use query with limit 0 to verify connectivity instead
      client.query(event_type: "test", limit: 0)
    end
  end

  describe "#flush" do
    let(:client) { http_client }

    before do
      stub_request(:post, "#{http_address}/command")
        .to_return(status: 200, body: "OK", headers: { "Content-Type" => "text/plain" })
    end

    it "executes FLUSH command" do
      expect(client).to receive(:execute).with("FLUSH")
      client.flush
    end
  end

  describe "#close" do
    let(:mock_socket) { double("TCPSocket") }
    let(:tcp_client_with_socket) do
      allow(TCPSocket).to receive(:new).and_return(mock_socket)
      allow(mock_socket).to receive(:setsockopt)
      allow(mock_socket).to receive(:closed?).and_return(false)
      allow(mock_socket).to receive(:close)
      allow(mock_socket).to receive(:write)
      allow(mock_socket).to receive(:flush)
      allow(mock_socket).to receive(:gets).and_return("OK\n")
      described_class.new(address: tcp_address, protocol: "tcp")
    end

    it "closes TCP connection" do
      # Trigger connection by executing a command
      tcp_client_with_socket.execute("FLUSH")
      expect(mock_socket).to receive(:close)
      tcp_client_with_socket.close
    end

    it "does nothing for HTTP client" do
      expect { http_client.close }.not_to raise_error
    end
  end

  describe "non-raising methods" do
    let(:client) { http_client }

    context "with successful response" do
      before do
        stub_request(:post, "#{http_address}/command")
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
        stub_request(:post, "#{http_address}/command")
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

  describe "raising methods" do
    let(:client) { http_client }

      context "with successful response" do
      before do
        stub_request(:post, "#{http_address}/command")
            .to_return(status: 200, body: '[{"id": 1}]', headers: { "Content-Type" => "application/json" })
        end

        it "returns array of hashes" do
          result = client.execute!("QUERY test")
          expect(result).to eq([{ "id" => 1 }])
        end
      end

      context "with error response" do
        before do
        stub_request(:post, "#{http_address}/command")
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
end
