require "spec_helper"

RSpec.describe SnelDB::Client do
  describe "with TCP transport" do
    let(:tcp_url) { "tcp://localhost:8086" }
    let(:client) do
      described_class.new(
        base_url: tcp_url,
        user_id: "test_user",
        secret_key: "test_secret"
      )
    end

    describe "#initialize" do
      it "creates TCP transport" do
        expect(client.transport).to be_a(SnelDB::Transport::TCP)
        expect(client.transport.host).to eq("localhost")
        expect(client.transport.port).to eq(8086)
      end
    end

    describe "#authenticate!" do
      let(:socket) { instance_double(TCPSocket) }

      before do
        allow(TCPSocket).to receive(:new).and_return(socket)
        allow(socket).to receive(:setsockopt)
        allow(socket).to receive(:closed?).and_return(false)
        allow(socket).to receive(:puts)
        allow(socket).to receive(:flush)
        allow(socket).to receive(:gets).and_return("OK TOKEN abc123def456\n")
        allow(IO).to receive(:select).and_return([[socket], [], []])
      end

      it "authenticates and stores session token" do
        token = client.authenticate!

        expect(token).to eq("abc123def456")
        expect(client.auth_manager.session_token).to eq("abc123def456")
        expect(client.transport.session_token).to eq("abc123def456")
      end

      it "sends AUTH command with correct format" do
        client.authenticate!

        expect(socket).to have_received(:puts) do |command|
          expect(command).to match(/^AUTH test_user:[0-9a-f]{64}$/)
        end
      end
    end

    describe "#execute!" do
      let(:socket) { instance_double(TCPSocket) }

      before do
        allow(TCPSocket).to receive(:new).and_return(socket)
        allow(socket).to receive(:setsockopt)
        allow(socket).to receive(:closed?).and_return(false)
        allow(socket).to receive(:puts)
        allow(socket).to receive(:flush)
        allow(socket).to receive(:gets).and_return("OK\n")
        allow(IO).to receive(:select).and_return([[socket], [], []])
      end

      context "without authentication" do
        it "formats command with inline authentication" do
          client.execute!("PING")

          expect(socket).to have_received(:puts) do |command|
            expect(command).to match(/^test_user:[0-9a-f]{64}:PING$/)
          end
        end
      end

      context "with session token" do
        before do
          client.transport.session_token = "token123"
          client.auth_manager.session_token = "token123"
        end

        it "formats command with TOKEN suffix" do
          client.execute!("PING")

          expect(socket).to have_received(:puts) do |command|
            expect(command).to eq("PING TOKEN token123")
          end
        end
      end

      context "with connection-scoped authentication" do
        before do
          client.transport.authenticated_user_id = "test_user"
          # Mock successful AUTH first
          allow(socket).to receive(:gets).and_return("OK TOKEN token123\n", "200 OK\n")
          allow(IO).to receive(:select).and_return([[socket], [], []], [[socket], [], []], [])
          client.authenticate!
          allow(socket).to receive(:gets).and_return("200 OK\n")
          allow(IO).to receive(:select).and_return([[socket], [], []], [])
        end

        it "formats command with TOKEN suffix after authentication" do
          client.execute!("PING")

          # Verify PING command was sent with TOKEN
          expect(socket).to have_received(:puts).at_least(:once) do |command|
            command == "PING TOKEN token123"
          end
        end
      end

      it "parses OK response" do
        allow(socket).to receive(:gets).and_return("200 OK\n", "Success\n")
        allow(IO).to receive(:select).and_return([[socket], [], []], [])
        result = client.execute!("PING")
        expect(result).to be_an(Array)
      end

      it "parses error response with ERROR: prefix" do
        allow(socket).to receive(:gets).and_return("ERROR: Invalid command\n")
        allow(IO).to receive(:select).and_return([[socket], [], []])
        expect { client.execute!("INVALID") }.to raise_error(SnelDB::CommandError, /Invalid command/)
      end

      it "parses HTTP-style error response (400)" do
        allow(socket).to receive(:gets).and_return("400 Bad Request\n")
        allow(IO).to receive(:select).and_return([[socket], [], []])
        expect { client.execute!("INVALID") }.to raise_error(SnelDB::CommandError)
      end

      it "parses HTTP-style error response (403)" do
        allow(socket).to receive(:gets).and_return("403 Permission denied\n")
        allow(IO).to receive(:select).and_return([[socket], [], []])
        expect { client.execute!("FORBIDDEN") }.to raise_error(SnelDB::AuthorizationError)
      end

      it "parses HTTP-style error response (500)" do
        allow(socket).to receive(:gets).and_return("500 Internal server error\n")
        allow(IO).to receive(:select).and_return([[socket], [], []])
        expect { client.execute!("ERROR") }.to raise_error(SnelDB::ServerError)
      end
    end

    describe "#store" do
      let(:socket) { instance_double(TCPSocket) }

      before do
        allow(TCPSocket).to receive(:new).and_return(socket)
        allow(socket).to receive(:setsockopt)
        allow(socket).to receive(:closed?).and_return(false)
        allow(socket).to receive(:puts)
        allow(socket).to receive(:flush)
        allow(socket).to receive(:gets).and_return("OK\n")
        allow(IO).to receive(:select).and_return([[socket], [], []])
      end

      it "sends STORE command with authentication" do
        client.store(
          event_type: "order_created",
          context_id: "customer-123",
          payload: { id: 1, amount: 99.99 }
        )

        expect(socket).to have_received(:puts) do |command|
          expect(command).to include("STORE order_created")
          expect(command).to include("customer-123")
          expect(command).to include("test_user:")
        end
      end
    end

    describe "#query" do
      let(:socket) { instance_double(TCPSocket) }

      context "with streaming JSON batch response" do
        let(:schema_frame) do
          '{"type":"schema","columns":[{"name":"id","logical_type":"Integer"},{"name":"amount","logical_type":"Float"}]}'
        end
        let(:batch_frame) do
          '{"type":"batch","rows":[[1,99.99],[2,149.99]]}'
        end
        let(:end_frame) do
          '{"type":"end","row_count":2}'
        end

        before do
          allow(TCPSocket).to receive(:new).and_return(socket)
          allow(socket).to receive(:setsockopt)
          allow(socket).to receive(:closed?).and_return(false)
          allow(socket).to receive(:puts)
          allow(socket).to receive(:flush)
          allow(socket).to receive(:gets).and_return(
            "#{schema_frame}\n",
            "#{batch_frame}\n",
            "#{end_frame}\n"
          )
          allow(IO).to receive(:select).and_return(
            [[socket], [], []],
            [[socket], [], []],
            [[socket], [], []],
            []
          )
        end

        it "sends QUERY command with authentication" do
          client.query(event_type: "order_created")

          expect(socket).to have_received(:puts) do |command|
            expect(command).to include("QUERY order_created")
            expect(command).to include("test_user:")
          end
        end

        it "parses streaming JSON batch response correctly" do
          result = client.query(event_type: "order_created")
          expect(result[:success]).to be true
          expect(result[:data]).to be_an(Array)
          expect(result[:data].length).to eq(2)
          expect(result[:data].first).to include("id" => 1, "amount" => 99.99)
          expect(result[:data].last).to include("id" => 2, "amount" => 149.99)
        end
      end

      context "with empty result set" do
        let(:schema_frame) do
          '{"type":"schema","columns":[{"name":"id","logical_type":"Integer"}]}'
        end
        let(:end_frame) do
          '{"type":"end","row_count":0}'
        end

        before do
          allow(TCPSocket).to receive(:new).and_return(socket)
          allow(socket).to receive(:setsockopt)
          allow(socket).to receive(:closed?).and_return(false)
          allow(socket).to receive(:puts)
          allow(socket).to receive(:flush)
          allow(socket).to receive(:gets).and_return(
            "#{schema_frame}\n",
            "#{end_frame}\n"
          )
          allow(IO).to receive(:select).and_return(
            [[socket], [], []],
            [[socket], [], []],
            []
          )
        end

        it "returns empty array" do
          result = client.query(event_type: "order_created")
          expect(result[:success]).to be true
          expect(result[:data]).to eq([])
        end
      end

      context "with HTTP-style error response" do
        before do
          allow(TCPSocket).to receive(:new).and_return(socket)
          allow(socket).to receive(:setsockopt)
          allow(socket).to receive(:closed?).and_return(false)
          allow(socket).to receive(:puts)
          allow(socket).to receive(:flush)
          allow(socket).to receive(:gets).and_return("400 Permission denied\n")
          allow(IO).to receive(:select).and_return([[socket], [], []])
        end

        it "handles error response correctly" do
          result = client.query(event_type: "order_created")
          expect(result[:success]).to be false
          expect(result[:error]).to be_a(SnelDB::CommandError)
        end
      end
    end

    describe "#ping" do
      let(:socket) { instance_double(TCPSocket) }

      context "with successful PONG response" do
        before do
          allow(TCPSocket).to receive(:new).and_return(socket)
          allow(socket).to receive(:setsockopt)
          allow(socket).to receive(:closed?).and_return(false)
          allow(socket).to receive(:puts)
          allow(socket).to receive(:flush)
          allow(socket).to receive(:gets).and_return("200 OK\n", "PONG\n")
          allow(IO).to receive(:select).and_return([[socket], [], []], [])
        end

        it "returns success hash" do
          result = client.ping
          expect(result[:success]).to be true
          expect(result[:error]).to be_nil
        end
      end

      context "with connection error" do
        before do
          allow(TCPSocket).to receive(:new).and_raise(Errno::ECONNREFUSED.new("Connection refused"))
        end

        it "returns error hash with ConnectionError" do
          result = client.ping
          expect(result[:success]).to be false
          expect(result[:error]).to be_a(SnelDB::ConnectionError)
        end
      end

      context "with invalid response (no PONG)" do
        before do
          allow(TCPSocket).to receive(:new).and_return(socket)
          allow(socket).to receive(:setsockopt)
          allow(socket).to receive(:closed?).and_return(false)
          allow(socket).to receive(:puts)
          allow(socket).to receive(:flush)
          allow(socket).to receive(:gets).and_return("200 OK\n")
          allow(IO).to receive(:select).and_return([[socket], [], []], [])
        end

        it "returns error hash" do
          result = client.ping
          expect(result[:success]).to be false
          expect(result[:error]).to be_a(SnelDB::ConnectionError)
          expect(result[:error].message).to include("PONG")
        end
      end
    end

    describe "#ping!" do
      let(:socket) { instance_double(TCPSocket) }

      context "with successful PONG response" do
        before do
          allow(TCPSocket).to receive(:new).and_return(socket)
          allow(socket).to receive(:setsockopt)
          allow(socket).to receive(:closed?).and_return(false)
          allow(socket).to receive(:puts)
          allow(socket).to receive(:flush)
          allow(socket).to receive(:gets).and_return("200 OK\n", "PONG\n")
          allow(IO).to receive(:select).and_return([[socket], [], []], [])
        end

        it "returns array of hashes" do
          result = client.ping!
          expect(result).to be_an(Array)
        end
      end

      context "with connection error" do
        before do
          allow(TCPSocket).to receive(:new).and_raise(Errno::ECONNREFUSED.new("Connection refused"))
        end

        it "raises ConnectionError" do
          expect { client.ping! }.to raise_error(SnelDB::ConnectionError)
        end
      end

      context "with invalid response (no PONG)" do
        before do
          allow(TCPSocket).to receive(:new).and_return(socket)
          allow(socket).to receive(:setsockopt)
          allow(socket).to receive(:closed?).and_return(false)
          allow(socket).to receive(:puts)
          allow(socket).to receive(:flush)
          allow(socket).to receive(:gets).and_return("200 OK\n")
          allow(IO).to receive(:select).and_return([[socket], [], []], [])
        end

        it "raises ConnectionError" do
          expect { client.ping! }.to raise_error(SnelDB::ConnectionError, /PONG/)
        end
      end
    end

    describe "#close" do
      let(:socket) { instance_double(TCPSocket) }

      before do
        allow(TCPSocket).to receive(:new).and_return(socket)
        allow(socket).to receive(:setsockopt)
        allow(socket).to receive(:closed?).and_return(false)
        allow(socket).to receive(:close)
      end

      it "closes TCP connection" do
        client.transport.connect
        client.close

        expect(socket).to have_received(:close)
      end
    end
  end
end
