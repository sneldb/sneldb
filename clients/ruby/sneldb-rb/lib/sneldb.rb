require_relative "sneldb/version"
require_relative "sneldb/errors"
require_relative "sneldb/transport/base"
require_relative "sneldb/transport/http"
require_relative "sneldb/transport/tcp"
require_relative "sneldb/auth"
require_relative "sneldb/client"

# Rails integration (optional)
if defined?(Rails)
  require_relative "sneldb/rails"
  require_relative "sneldb/active_record"
end

module SnelDB
  # Main entry point for the SnelDB Ruby client
end

