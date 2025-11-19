require "spec_helper"

RSpec.describe SnelDB::Transport do
  describe "HTTP transport" do
    let(:transport) { SnelDB::Transport::HTTP.new(host: "localhost", port: 8085, use_ssl: false) }

    describe "#execute" do
      it "sends POST request to /command endpoint" do
        stub_request(:post, "http://localhost:8085/command")
          .with(body: "PING", headers: { "Content-Type" => "text/plain" })
          .to_return(status: 200, body: "OK", headers: { "Content-Type" => "text/plain" })

        result = transport.execute("PING")
        expect(result[:status]).to eq(200)
        expect(result[:body]).to eq("OK")
      end

      it "includes custom headers" do
        stub_request(:post, "http://localhost:8085/command")
          .with(headers: { "X-Custom-Header" => "value" })
          .to_return(status: 200, body: "OK")

        result = transport.execute("PING", headers: { "X-Custom-Header" => "value" })
        expect(result[:status]).to eq(200)
      end

      it "handles HTTPS" do
        https_transport = SnelDB::Transport::HTTP.new(host: "localhost", port: 443, use_ssl: true)
        stub_request(:post, "https://localhost:443/command")
          .to_return(status: 200, body: "OK")

        result = https_transport.execute("PING")
        expect(result[:status]).to eq(200)
      end

      it "raises ConnectionError on connection refused" do
        stub_request(:post, "http://localhost:8085/command")
          .to_raise(Errno::ECONNREFUSED.new("Connection refused"))

        expect { transport.execute("PING") }.to raise_error(SnelDB::ConnectionError)
      end

      it "raises ConnectionError on timeout" do
        stub_request(:post, "http://localhost:8085/command")
          .to_timeout

        expect { transport.execute("PING") }.to raise_error(SnelDB::ConnectionError)
      end
    end
  end

  describe "TCP transport" do
    let(:transport) { SnelDB::Transport::TCP.new(host: "localhost", port: 8086, use_ssl: false) }

    describe "#connect" do
      it "creates TCP socket connection" do
        # Mock TCPSocket to avoid actual network connection
        socket = instance_double(TCPSocket, closed?: false)
        allow(TCPSocket).to receive(:new).and_return(socket)
        allow(socket).to receive(:setsockopt)
        allow(socket).to receive(:puts)
        allow(socket).to receive(:flush)
        allow(socket).to receive(:gets).and_return("OK\n")
        allow(IO).to receive(:select).and_return([[socket], [], []])

        transport.connect
        expect(transport.socket).not_to be_nil
      end

      it "raises ConnectionError on connection failure" do
        allow(TCPSocket).to receive(:new).and_raise(Errno::ECONNREFUSED.new("Connection refused"))

        expect { transport.connect }.to raise_error(SnelDB::ConnectionError)
      end
    end

    describe "#execute" do
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

      it "sends command and reads response" do
        transport.connect
        result = transport.execute("PING")

        expect(result[:status]).to eq(200)
        expect(result[:body]).to include("OK")
        expect(socket).to have_received(:puts).with("PING")
        expect(socket).to have_received(:flush)
      end

      it "handles error responses" do
        allow(socket).to receive(:gets).and_return("ERROR: Invalid command\n")
        transport.connect

        result = transport.execute("INVALID")
        expect(result[:status]).to eq(400)
        expect(result[:body]).to include("ERROR")
      end

      it "handles multi-line responses" do
        allow(socket).to receive(:gets).and_return("OK\n", "Line 1\n", "Line 2\n", nil)
        allow(IO).to receive(:select).and_return([[socket], [], []], [[socket], [], []], [[socket], [], []], nil)
        transport.connect

        result = transport.execute("QUERY test")
        expect(result[:status]).to eq(200)
        expect(result[:body]).to include("OK")
      end

      it "raises ConnectionError on connection reset" do
        transport.connect
        allow(socket).to receive(:puts).and_raise(Errno::ECONNRESET.new("Connection reset"))

        expect { transport.execute("PING") }.to raise_error(SnelDB::ConnectionError)
        expect(transport.socket).to be_nil
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

      it "closes socket connection" do
        transport.connect
        transport.close

        expect(socket).to have_received(:close)
        expect(transport.socket).to be_nil
        expect(transport.authenticated_user_id).to be_nil
        expect(transport.session_token).to be_nil
      end

      it "handles already closed socket" do
        allow(socket).to receive(:closed?).and_return(true)
        transport.instance_variable_set(:@socket, socket)

        expect { transport.close }.not_to raise_error
      end
    end
  end
end

