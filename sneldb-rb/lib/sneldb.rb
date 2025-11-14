require_relative "sneldb/version"
require_relative "sneldb/client"
require_relative "sneldb/errors"

# Rails integration (optional)
if defined?(Rails)
  require_relative "sneldb/rails"
  require_relative "sneldb/active_record"
end

module SnelDB
  # Main entry point for the SnelDB Ruby client
end

