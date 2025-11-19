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
          allow(socket).to receive(:gets).and_return("OK TOKEN token123\n", "OK\n")
          allow(IO).to receive(:select).and_return([[socket], [], []], [[socket], [], []])
          client.authenticate!
          allow(socket).to receive(:gets).and_return("OK\n")
        end

        it "formats command with signature prefix" do
          client.execute!("PING")

          expect(socket).to have_received(:puts) do |command|
            expect(command).to match(/^[0-9a-f]{64}:PING$/)
          end
        end
      end

      it "parses OK response" do
        result = client.execute!("PING")
        expect(result).to be_an(Array)
      end

      it "parses error response" do
        allow(socket).to receive(:gets).and_return("ERROR: Invalid command\n")
        result = client.execute!("INVALID")

        expect(result).to be_an(Array)
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

      before do
        allow(TCPSocket).to receive(:new).and_return(socket)
        allow(socket).to receive(:setsockopt)
        allow(socket).to receive(:closed?).and_return(false)
        allow(socket).to receive(:puts)
        allow(socket).to receive(:flush)
        allow(socket).to receive(:gets).and_return("OK\n", '[{"id": 1}]\n')
        allow(IO).to receive(:select).and_return([[socket], [], []], [[socket], [], []])
      end

      it "sends QUERY command with authentication" do
        client.query(event_type: "order_created")

        expect(socket).to have_received(:puts) do |command|
          expect(command).to include("QUERY order_created")
          expect(command).to include("test_user:")
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
