require "spec_helper"

RSpec.describe SnelDB::Auth::Manager do
  let(:user_id) { "test_user" }
  let(:secret_key) { "test_secret_key" }
  let(:auth_manager) { described_class.new(user_id: user_id, secret_key: secret_key) }

  describe "#initialize" do
    it "sets user_id and secret_key" do
      expect(auth_manager.user_id).to eq(user_id)
      expect(auth_manager.secret_key).to eq(secret_key)
    end

    it "allows nil credentials" do
      manager = described_class.new(user_id: nil, secret_key: nil)
      expect(manager.user_id).to be_nil
      expect(manager.secret_key).to be_nil
    end
  end

  describe "#compute_signature" do
    it "computes HMAC-SHA256 signature" do
      message = "STORE test FOR ctx1 PAYLOAD {\"id\":1}"
      signature = auth_manager.compute_signature(message)

      expect(signature).to match(/\A[0-9a-f]{64}\z/i)
    end

    it "trims message before computing signature" do
      message1 = "  STORE test  "
      message2 = "STORE test"
      sig1 = auth_manager.compute_signature(message1)
      sig2 = auth_manager.compute_signature(message2)

      expect(sig1).to eq(sig2)
    end

    it "raises error when secret_key is not set" do
      manager = described_class.new(user_id: user_id, secret_key: nil)
      expect { manager.compute_signature("test") }.to raise_error(SnelDB::AuthenticationError)
    end

    it "produces consistent signatures for same input" do
      message = "PING"
      sig1 = auth_manager.compute_signature(message)
      sig2 = auth_manager.compute_signature(message)

      expect(sig1).to eq(sig2)
    end
  end

  describe "#add_http_headers" do
    it "adds authentication headers" do
      command = "STORE test FOR ctx1 PAYLOAD {\"id\":1}"
      headers = auth_manager.add_http_headers(command)

      expect(headers["X-Auth-User"]).to eq(user_id)
      expect(headers["X-Auth-Signature"]).to match(/\A[0-9a-f]{64}\z/i)
    end

    it "merges with existing headers" do
      command = "PING"
      existing_headers = { "Content-Type" => "text/plain" }
      headers = auth_manager.add_http_headers(command, headers: existing_headers)

      expect(headers["Content-Type"]).to eq("text/plain")
      expect(headers["X-Auth-User"]).to eq(user_id)
    end

    it "returns headers unchanged when credentials not set" do
      manager = described_class.new(user_id: nil, secret_key: nil)
      headers = manager.add_http_headers("PING", headers: {})

      expect(headers).to be_empty
    end
  end

  describe "#format_tcp_command" do
    let(:transport) { instance_double(SnelDB::Transport::TCP) }
    let(:command) { "STORE test FOR ctx1 PAYLOAD {\"id\":1}" }

    before do
      allow(transport).to receive(:is_a?).with(SnelDB::Transport::TCP).and_return(true)
      allow(transport).to receive(:session_token).and_return(nil)
      allow(transport).to receive(:authenticated_user_id).and_return(nil)
    end

    context "with session token" do
      it "formats command with TOKEN suffix" do
        allow(transport).to receive(:session_token).and_return("abc123token")
        formatted = auth_manager.format_tcp_command(command, transport)

        expect(formatted).to eq("#{command} TOKEN abc123token")
      end

      it "uses auth_manager's session token if available" do
        auth_manager.session_token = "manager_token"
        allow(transport).to receive(:session_token).and_return(nil)
        formatted = auth_manager.format_tcp_command(command, transport)

        expect(formatted).to eq("#{command} TOKEN manager_token")
      end
    end

    context "with connection-scoped authentication" do
      it "formats command with signature prefix" do
        allow(transport).to receive(:authenticated_user_id).and_return(user_id)
        allow(transport).to receive(:session_token).and_return(nil)
        formatted = auth_manager.format_tcp_command(command, transport)

        expect(formatted).to start_with(auth_manager.compute_signature(command))
        expect(formatted).to include(":")
        expect(formatted).to end_with(command.strip)
      end
    end

    context "with inline format" do
      it "formats command as user_id:signature:command" do
        allow(transport).to receive(:session_token).and_return(nil)
        allow(transport).to receive(:authenticated_user_id).and_return(nil)
        formatted = auth_manager.format_tcp_command(command, transport)

        expect(formatted).to start_with("#{user_id}:")
        expect(formatted).to include(":")
        expect(formatted).to end_with(command.strip)
      end
    end

    context "without authentication" do
      it "returns command unchanged" do
        manager = described_class.new(user_id: nil, secret_key: nil)
        formatted = manager.format_tcp_command(command, transport)

        expect(formatted).to eq(command)
      end
    end
  end

  describe "#authenticate!" do
    let(:transport) { instance_double(SnelDB::Transport::TCP) }
    let(:response) { { status: 200, body: "OK TOKEN abc123def456" } }

    before do
      allow(transport).to receive(:is_a?).with(SnelDB::Transport::TCP).and_return(true)
      allow(transport).to receive(:connect)
      allow(transport).to receive(:execute).and_return(response)
      allow(transport).to receive(:session_token=)
      allow(transport).to receive(:authenticated_user_id=)
    end

    it "authenticates and returns session token" do
      token = auth_manager.authenticate!(transport)

      expect(token).to eq("abc123def456")
      expect(auth_manager.session_token).to eq("abc123def456")
      expect(auth_manager.authenticated_user_id).to eq(user_id)
    end

    it "sets token on transport" do
      auth_manager.authenticate!(transport)

      expect(transport).to have_received(:session_token=).with("abc123def456")
      expect(transport).to have_received(:authenticated_user_id=).with(user_id)
    end

    it "raises error when credentials not set" do
      manager = described_class.new(user_id: nil, secret_key: nil)
      expect { manager.authenticate!(transport) }.to raise_error(SnelDB::AuthenticationError)
    end

    it "raises error for non-TCP transport" do
      http_transport = instance_double(SnelDB::Transport::HTTP)
      allow(http_transport).to receive(:is_a?).with(SnelDB::Transport::TCP).and_return(false)

      expect { auth_manager.authenticate!(http_transport) }.to raise_error(SnelDB::AuthenticationError)
    end

    it "raises error on authentication failure" do
      allow(transport).to receive(:execute).and_return({ status: 401, body: "ERROR: Authentication failed" })

      expect { auth_manager.authenticate!(transport) }.to raise_error(SnelDB::AuthenticationError)
    end

    it "raises error when token not in response" do
      allow(transport).to receive(:execute).and_return({ status: 200, body: "OK" })

      expect { auth_manager.authenticate!(transport) }.to raise_error(SnelDB::AuthenticationError)
    end
  end

  describe "#clear" do
    it "clears session token and authenticated_user_id" do
      auth_manager.session_token = "token123"
      auth_manager.authenticated_user_id = "user123"

      auth_manager.clear

      expect(auth_manager.session_token).to be_nil
      expect(auth_manager.authenticated_user_id).to be_nil
    end
  end
end

