# SnelDB Ruby Client

A Ruby gem for interacting with [SnelDB](https://sneldb.com) event database via HTTP or TCP using the standard command format. Supports authentication, RBAC, fine-grained permissions, and all SnelDB commands.

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

```ruby
require 'sneldb'

# Create an HTTP client
client = SnelDB::Client.new(
  base_url: "http://localhost:8085",
  user_id: "your_user_id",      # Optional
  secret_key: "your_secret_key" # Optional
)

# Or create a TCP client for high-throughput scenarios
tcp_client = SnelDB::Client.new(
  base_url: "tcp://localhost:8086",
  user_id: "your_user_id",
  secret_key: "your_secret_key"
)
```

### Database Initialization

The client can automatically initialize the database if it's not already initialized. This is useful for first-time setup:

```ruby
# Auto-initialize on client creation
client = SnelDB::Client.new(
  base_url: "http://localhost:8085",
  auto_initialize: true,
  initial_admin_user: "admin",        # From config: initial_admin_user
  initial_admin_key: "admin-key-123"  # From config: initial_admin_key
)

# Or manually check and initialize
client = SnelDB::Client.new(base_url: "http://localhost:8085")

if !client.initialized?
  client.ensure_initialized!(
    admin_user_id: "admin",
    admin_secret_key: "admin-key-123"
  )
end
```

**Note:** Database initialization requires that authentication is either bypassed (`bypass_auth = true` in config) or that the server allows user creation without authentication. The server automatically bootstraps the admin user on startup if configured in `prod.toml` (lines 56-58), but the client can also ensure initialization if needed.

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
response = client.execute("PING")
response = client.execute("FLUSH")
```

### User Management

**Note:** User management commands require admin authentication.

```ruby
# Create a user (admin only)
# Returns the generated secret key in the response
result = client.create_user(user_id: "new_user")
if result[:success]
  # Extract secret key from response
  secret_key_line = result[:data].find { |line| line[:raw].to_s.include?("Secret key:") }
  puts "Secret key: #{secret_key_line}"
end

# Create a user with a specific key
client.create_user(user_id: "new_user", secret_key: "my_secret_key")

# List all users
users = client.list_users
users[:data].each do |user_line|
  puts user_line[:raw] # e.g., "admin: active" or "user1: inactive"
end

# Revoke a user's key (marks user as inactive)
client.revoke_key(user_id: "new_user")

# Complete workflow: Create user and grant permissions
client.create_user!(user_id: "api_client")
client.grant_permission!(
  user_id: "api_client",
  permissions: ["read", "write"],
  event_types: ["order_created", "payment_succeeded"]
)
```

**Important:** The `CREATE USER` command creates regular users without roles. To create an admin user, you need to:
1. Use the server's bootstrap feature (configured in `prod.toml` with `initial_admin_user` and `initial_admin_key`)
2. Or use the client's `ensure_initialized!` method which creates the admin user from config
3. Regular users can be granted permissions but cannot be assigned roles via the command interface yet

### Permission Management

```ruby
# Grant read permission on an event type
client.grant_permission(
  user_id: "api_client",
  permissions: ["read"],
  event_types: ["order_created"]
)

# Grant read and write permissions on multiple event types
client.grant_permission(
  user_id: "api_client",
  permissions: ["read", "write"],
  event_types: ["order_created", "payment_succeeded"]
)

# Revoke specific permissions
client.revoke_permission(
  user_id: "api_client",
  permissions: ["write"],
  event_types: ["order_created"]
)

# Revoke all permissions for an event type
client.revoke_permission(
  user_id: "api_client",
  event_types: ["order_created"]
)

# Show permissions for a user
client.show_permissions(user_id: "api_client")
```

### Materialized Views

```ruby
# Remember a query as a materialized view
client.remember(
  name: "recent_orders",
  event_type: "order_created",
  where: 'amount > 100',
  limit: 1000
)

# Show a materialized view
client.show_materialized(name: "recent_orders")
```

### Batch Operations

```ruby
# Execute multiple commands in a batch
client.batch([
  "STORE order_created FOR customer-1 PAYLOAD {\"id\": 1}",
  "STORE order_created FOR customer-2 PAYLOAD {\"id\": 2}"
])
```

### Query Comparison

```ruby
# Compare multiple queries
client.compare([
  "QUERY order_created WHERE amount > 100",
  "QUERY order_created WHERE amount > 200"
])
```

### PLOT Queries

```ruby
# Execute a PLOT query for data visualization
client.plot("QUERY order_created WHERE amount > 100")
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

The client supports multiple authentication methods depending on the transport protocol:

### HTTP Authentication

For HTTP connections, authentication is done via headers (`X-Auth-User` and `X-Auth-Signature`). The signature is computed using HMAC-SHA256 of the command body with your secret key.

```ruby
client = SnelDB::Client.new(
  base_url: "http://localhost:8085",
  user_id: "my_user",
  secret_key: "my_secret_key"
)
```

### TCP Authentication

For TCP connections, the client supports three authentication methods:

#### 1. Session Token Authentication (Recommended for high-throughput)

Authenticate once with the `AUTH` command to get a session token, then use it for subsequent commands:

```ruby
tcp_client = SnelDB::Client.new(
  base_url: "tcp://localhost:8086",
  user_id: "my_user",
  secret_key: "my_secret_key"
)

# Authenticate and get session token
token = tcp_client.authenticate!

# Subsequent commands automatically use the token
tcp_client.store(
  event_type: "order_created",
  context_id: "customer-123",
  payload: { id: 1, amount: 99.99 }
)
```

#### 2. Connection-Scoped Authentication

After authenticating with `AUTH`, you can send commands with just a signature prefix:

```ruby
tcp_client.authenticate!
# Commands are automatically formatted with signature prefix
```

#### 3. Inline Format (Per-Command)

Each command includes user_id and signature:

```ruby
# Commands are automatically formatted as: user_id:signature:command
```

### RBAC and Permissions

SnelDB supports Role-Based Access Control (RBAC) with fine-grained permissions:

- **Admin Role**: Users with the `admin` role have full access to all event types
- **Fine-Grained Permissions**: Users can be granted read and/or write permissions per event type
- **Permission Checks**: Permissions are automatically checked before STORE (write) and QUERY (read) operations

Admin users can:
- Create and manage users
- Grant and revoke permissions
- Define event schemas
- Access all event types regardless of permissions

## Response Format

By default, responses are returned as text. You can configure the output format:

```ruby
client = SnelDB::Client.new(
  base_url: "http://localhost:8085",
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

### Event Commands
- `DEFINE` - Define event type schemas
- `STORE` - Store events
- `QUERY` / `FIND` - Query events with filters
- `REPLAY` - Replay events for a context
- `PLOT` - Execute PLOT queries for visualization

### System Commands
- `FLUSH` - Flush memtable to disk
- `PING` - Health check
- `BATCH` - Execute multiple commands in a batch
- `COMPARE` - Compare multiple queries

### User Management Commands (Admin Only)
- `CREATE USER` - Create authentication users
- `LIST USERS` - List all users
- `REVOKE KEY` - Revoke a user's key

### Permission Management Commands (Admin Only)
- `GRANT` - Grant permissions to users for event types
- `REVOKE` - Revoke permissions from users for event types
- `SHOW PERMISSIONS` - Show permissions for a user

### Materialized Views
- `REMEMBER` - Remember a query as a materialized view
- `SHOW MATERIALIZED` - Show a materialized view

### Authentication Commands (TCP Only)
- `AUTH` - Authenticate and get session token

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

