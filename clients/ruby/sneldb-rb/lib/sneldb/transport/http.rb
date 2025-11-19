require "net/http"
require "uri"
require_relative "base"

module SnelDB
  module Transport
    # HTTP transport implementation
    class HTTP < Base
      def initialize(host:, port:, use_ssl: false, read_timeout: 60)
        super(host: host, port: port, use_ssl: use_ssl)
        @read_timeout = read_timeout
      end

      def execute(command, headers: {})
        uri = URI("#{scheme}://#{@host}:#{@port}/command")
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = @use_ssl
        http.read_timeout = @read_timeout

        request = Net::HTTP::Post.new(uri.path)
        request.body = command
        request["Content-Type"] = "text/plain"

        # Add custom headers
        headers.each { |k, v| request[k] = v }

        response = http.request(request)

        {
          status: response.code.to_i,
          body: response.body,
          headers: response.to_hash
        }
      rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH, Errno::ETIMEDOUT => e
        raise ConnectionError, "Cannot connect to server at #{scheme}://#{@host}:#{@port}: #{e.message}"
      rescue SocketError => e
        raise ConnectionError, "Network error: #{e.message}"
      rescue OpenSSL::SSL::SSLError => e
        raise ConnectionError, "SSL error: #{e.message}"
      rescue Net::ReadTimeout => e
        raise ConnectionError, "Request timeout: #{e.message}"
      rescue Net::OpenTimeout => e
        raise ConnectionError, "Connection timeout: #{e.message}"
      rescue => e
        raise ConnectionError, "Network error: #{e.class} - #{e.message}"
      end

      private

      def scheme
        @use_ssl ? "https" : "http"
      end
    end
  end
end

