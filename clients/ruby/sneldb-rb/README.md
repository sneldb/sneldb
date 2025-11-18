# SnelDB Ruby Client

A Ruby gem for interacting with [SnelDB](https://sneldb.com) event database via TCP (default) or HTTP using the standard command format.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'sneldb'
```

And then execute:

```bash
$ bundle install
```

Or install it yourself as:

```bash
$ gem install sneldb
```

## Usage

### Basic Setup

The client supports both TCP (default) and HTTP protocols. TCP is recommended for better performance.

#### TCP Connection (Default)

```ruby
require 'sneldb'

# Create a TCP client (default protocol)
client = SnelDB::Client.new(
  address: "localhost:8086",     # TCP address format: host:port
  user_id: "your_user_id",       # Optional
  secret_key: "your_secret_key"   # Optional
)

# Or explicitly specify protocol
client = SnelDB::Client.new(
  address: "localhost:8086",
  protocol: "tcp",
  user_id: "your_user_id",
  secret_key: "your_secret_key"
)
```

#### HTTP Connection

```ruby
require 'sneldb'

# Create an HTTP client
client = SnelDB::Client.new(
  address: "http://localhost:8085",  # HTTP URL format
  protocol: "http",
  user_id: "your_user_id",           # Optional
  secret_key: "your_secret_key"       # Optional
)

```

#### Protocol Selection

The protocol is automatically detected from the address format:

- `host:port` → TCP (e.g., `"localhost:8086"`)
- `http://host:port` or `https://host:port` → HTTP (e.g., `"http://localhost:8085"`)

You can also explicitly specify the protocol:

```ruby
# TCP (default)
client = SnelDB::Client.new(address: "localhost:8086", protocol: "tcp")

# HTTP
client = SnelDB::Client.new(address: "localhost:8085", protocol: "http")
```

### Define a Schema

```ruby
# Define an event type schema
client.define(
  event_type: "order_created",
  fields: {
    "id" => "int",
    "amount" => "float",
    "currency" => "string",
    "created_at" => "datetime"
  }
)

# With version
client.define(
  event_type: "order_created",
  version: 1,
  fields: {
    "id" => "int",
    "amount" => "float"
  }
)
```

### Store Events

```ruby
# Store an event
client.store(
  event_type: "order_created",
  context_id: "customer-123",
  payload: {
    id: 42,
    amount: 99.99,
    currency: "USD",
    created_at: "2025-01-15T10:30:00Z"
  }
)
```

### Query Events

```ruby
# Simple query
response = client.query(
  event_type: "order_created",
  where: 'amount >= 50'
)

# Query with context and time filter
response = client.query(
  event_type: "order_created",
  context_id: "customer-123",
  since: "2025-01-01T00:00:00Z",
  using: "created_at",
  where: 'currency = "USD"',
  limit: 100
)

# Query with specific return fields
response = client.query(
  event_type: "order_created",
  return_fields: ["id", "amount", "currency"],
  where: 'amount > 100'
)
```

### Replay Events

```ruby
# Replay all events for a context
response = client.replay(context_id: "customer-123")

# Replay specific event type
response = client.replay(
  context_id: "customer-123",
  event_type: "order_created",
  since: "2025-01-01T00:00:00Z"
)
```

### Execute Raw Commands

```ruby
# Execute any command string directly
response = client.execute("QUERY order_created WHERE amount > 100 LIMIT 10")
# Note: PING command is not available - use query to verify connectivity
response = client.query(event_type: "your_event_type", limit: 1)
response = client.execute("FLUSH")
```

### User Management

```ruby
# Create a user
client.create_user(user_id: "new_user")

# Create a user with a specific key
client.create_user(user_id: "new_user", secret_key: "my_secret_key")

# List all users
client.list_users

# Revoke a user's key
client.revoke_key(user_id: "new_user")

# Grant permissions to a user
client.grant_permission(
  permissions: ["read", "write"],
  event_types: ["order_created", "payment_succeeded"],
  user_id: "api_client"
)

# Grant read-only permission
client.grant_permission(
  permissions: ["read"],
  event_types: ["order_created"],
  user_id: "readonly_client"
)

# Revoke specific permissions
client.revoke_permission(
  event_types: ["order_created"],
  user_id: "api_client",
  permissions: ["write"]  # Only revoke write, keep read
)

# Revoke all permissions for event types
client.revoke_permission(
  event_types: ["order_created", "payment_succeeded"],
  user_id: "api_client"
  # permissions: nil means revoke all
)

# Show permissions for a user
client.show_permissions(user_id: "api_client")
```

### Error Handling

```ruby
begin
  client.store(
    event_type: "order_created",
    context_id: "customer-123",
    payload: { id: 1, amount: 50.0 }
  )
rescue SnelDB::AuthenticationError => e
  puts "Authentication failed: #{e.message}"
rescue SnelDB::CommandError => e
  puts "Command error: #{e.message}"
rescue SnelDB::ConnectionError => e
  puts "Connection error: #{e.message}"
rescue SnelDB::Error => e
  puts "Error: #{e.message}"
end
```

## Authentication

The client supports authentication for both TCP and HTTP protocols. The signature is computed using HMAC-SHA256 of the command body with your secret key.

#### TCP Authentication

TCP supports multiple authentication methods:

1. **Connection-scoped authentication** (recommended): Authenticate once with `AUTH` command, then use session tokens for subsequent commands
2. **Per-command authentication**: Include signature with each command

The client automatically uses connection-scoped authentication when credentials are provided:

```ruby
# TCP with authentication (uses connection-scoped auth automatically)
client = SnelDB::Client.new(
  address: "localhost:8086",
  protocol: "tcp",
  user_id: "my_user",
  secret_key: "my_secret_key"
)
```

#### HTTP Authentication

HTTP uses headers (`X-Auth-User` and `X-Auth-Signature`) for authentication:

```ruby
client = SnelDB::Client.new(
  address: "http://localhost:8085",
  protocol: "http",
  user_id: "my_user",
  secret_key: "my_secret_key"
)
```

### Connection Management

#### TCP Connections

TCP connections are persistent and automatically managed. The connection is established on first use and reused for subsequent commands. You can manually close the connection:

```ruby
client = SnelDB::Client.new(address: "localhost:8086")
# ... use client ...
client.close  # Close TCP connection
```

TCP connections support session tokens for high-performance authentication. After initial authentication, the client automatically uses session tokens to avoid computing HMAC signatures for each command.

#### HTTP Connections

HTTP connections are stateless - each command creates a new HTTP request. No connection management is needed.

## Response Format

By default, responses are returned as text. You can configure the output format:

```ruby
client = SnelDB::Client.new(
  address: "http://localhost:8085",
  output_format: "json" # or "text" (default) or "arrow"
)
```

## Rails Integration

The gem includes full Rails integration with generators and ActiveRecord support.

### Setup

First, install the initializer:

```bash
rails generate sneldb:install
```

This creates `config/initializers/sneldb.rb` with configuration. You can also set these via environment variables:

- `SNELDB_URL` - SnelDB server URL (default: `http://localhost:8085`)
- `SNELDB_USER_ID` - User ID for authentication
- `SNELDB_SECRET_KEY` - Secret key for authentication
- `SNELDB_OUTPUT_FORMAT` - Output format: `text`, `json`, or `arrow` (default: `text`)

### Generate Event Definitions

Generate an event definition with a Rails generator:

```bash
rails generate sneldb:event OrderCreated id:uuid amount:float currency:string created_at:datetime
```

This creates an event definition in `config/initializers/sneldb_events.rb`. You can also specify a schema version:

```bash
rails generate sneldb:event OrderCreated id:uuid amount:float --version=2
```

After generating events, register them with SnelDB:

```bash
rails runner SnelDB::Rails.define_events
```

### Using ActiveRecord Models

Include the SnelDB concern in your models to automatically store events after commits:

```ruby
class Order < ApplicationRecord
  # Define events to store when this model changes
  sneldb_event :order_created, on: :create do
    {
      id: id,
      amount: total_amount,
      currency: currency,
      created_at: created_at.iso8601
    }
  end

  sneldb_event :order_updated, on: :update do
    {
      id: id,
      amount: total_amount,
      status: status,
      updated_at: updated_at.iso8601
    }
  end

  sneldb_event :order_cancelled, on: :destroy do
    {
      id: id,
      cancelled_at: Time.current.iso8601
    }
  end

  # Optional: Override context ID generation
  def sneldb_context_id
    "customer-#{customer_id}"
  end
end
```

Now, whenever an `Order` is created, updated, or destroyed, the corresponding event will be automatically stored in SnelDB after the transaction commits.

### Manual Event Definitions

You can also define events manually in `config/initializers/sneldb_events.rb`:

```ruby
# config/initializers/sneldb_events.rb
SnelDB::Rails.define_event(
  event_type: "order_created",
  fields: {
    "id" => "uuid",
    "amount" => "float",
    "currency" => "string",
    "created_at" => "datetime"
  }
)
```

### Storing Events Manually

You can store events manually using the Rails helper:

```ruby
SnelDB::Rails.store_event(
  event_type: :order_created,
  context_id: "customer-123",
  payload: {
    id: order.id,
    amount: order.total_amount,
    currency: order.currency
  }
)
```

### Context ID Customization

By default, the gem tries to infer the context ID from common patterns (`user_id`, `customer_id`, `account_id`). You can override this in your models:

```ruby
class Order < ApplicationRecord
  def sneldb_context_id
    # Custom context ID logic
    "tenant-#{tenant_id}/customer-#{customer_id}"
  end
end
```

## Command Reference

The client supports all SnelDB commands:

- `DEFINE` - Define event type schemas
- `STORE` - Store events
- `QUERY` - Query events with filters
- `REPLAY` - Replay events for a context
- `FLUSH` - Flush memtable to disk
- `CREATE USER` - Create authentication users
- `LIST USERS` - List all users
- `REVOKE KEY` - Revoke a user's key
- `GRANT` - Grant permissions to users for event types
- `REVOKE` - Revoke permissions from users for event types
- `SHOW PERMISSIONS` - Show permissions for a user

For detailed command syntax, see the [SnelDB documentation](https://sneldb.com/commands.html).

## Development

After checking out the repo, run:

```bash
$ bundle install
$ rake spec
```

## Contributing

Bug reports and pull requests are welcome on GitHub.

## License

The gem is available as open source under the terms of the [MIT License](LICENSE).
