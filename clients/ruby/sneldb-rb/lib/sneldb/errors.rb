module SnelDB
  class Error < StandardError; end

  # Network/connection related errors
  class ConnectionError < Error; end

  # Authentication/authorization errors
  class AuthenticationError < Error; end
  class AuthorizationError < Error; end

  # Command/request errors
  class CommandError < Error; end
  class NotFoundError < Error; end

  # Response parsing errors
  class ParseError < Error; end

  # Server errors
  class ServerError < Error; end
end

