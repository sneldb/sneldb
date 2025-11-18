# User Management

## Purpose

SnelDB provides authentication and authorization through HMAC-based signatures and session tokens. User management commands allow you to create users, revoke their access keys, list all registered users, and manage fine-grained permissions for event types.

All commands require authentication. SnelDB supports multiple authentication methods:

- **Session tokens** (recommended for high-throughput): Authenticate once with `AUTH`, receive a token, then use it for subsequent commands
- **HMAC-SHA256 signatures**: Sign each command with a user's secret key
- **Connection-scoped authentication**: Authenticate once per connection, then send signed commands

User management commands (CREATE USER, REVOKE KEY, LIST USERS) and permission management commands (GRANT, REVOKE, SHOW PERMISSIONS) require admin privileges. This ensures that only authorized administrators can manage users and permissions.

## Authentication Overview

SnelDB supports multiple authentication methods to suit different use cases:

- **Session Token Authentication**: After authenticating with the `AUTH` command, you receive a session token that can be reused for multiple commands without re-signing. This is optimized for high-throughput scenarios, especially WebSocket connections.

- **HMAC-SHA256 Signature Authentication**: Each user has a secret key that is used to sign commands. The signature proves that the command was issued by someone who knows the secret key. This can be done per-command (inline format) or connection-scoped (after AUTH command).

### Authentication Formats

**1. Session Token Authentication (Recommended for high-throughput):**

After authenticating with the `AUTH` command, you receive a session token that can be used for subsequent commands:

```sneldb
AUTH user_id:signature
OK TOKEN <session_token>
```

Then use the token with subsequent commands:

```sneldb
STORE event_type FOR context_id PAYLOAD {...} TOKEN <session_token>
QUERY event_type WHERE field=value TOKEN <session_token>
```

- Tokens are session-based and expire after a configurable time (default: 5 minutes, configurable via `auth.session_token_expiry_seconds`)
- Tokens are 64-character hexadecimal strings (32 bytes)
- The token must be appended at the end of the command after `TOKEN`
- This method is optimized for high-throughput scenarios, especially WebSocket connections
- If a token is invalid or expired, the system falls back to other authentication methods

**2. TCP/UNIX/WebSocket (after AUTH command, connection-scoped):**

```sneldb
AUTH user_id:signature
signature:STORE event_type FOR context_id PAYLOAD {...}
```

**3. TCP/UNIX/WebSocket (inline format, per-command):**

```sneldb
user_id:signature:STORE event_type FOR context_id PAYLOAD {...}
```

**4. HTTP (header-based):**

```
X-Auth-User: user_id
X-Auth-Signature: signature
```

The signature is computed as: `HMAC-SHA256(secret_key, message)` where `message` is the command string being executed.

**Note:** WebSocket connections support all authentication formats (token, AUTH command, and inline format). Commands are sent as text messages over the WebSocket connection.

## CREATE USER

### Purpose

Create a new user with authentication credentials. The user will receive a secret key that can be used to sign commands.

### Form

```sneldb
CREATE USER <user_id:WORD or STRING> [ WITH KEY <secret_key:STRING> ]
```

### Constraints

- `<user_id>` must be non-empty and contain only alphanumeric characters, underscores, or hyphens.
- `<user_id>` is case-sensitive (e.g., `user1` ≠ `User1`).
- If `WITH KEY` is omitted, a random 64-character hexadecimal secret key is generated.
- If `WITH KEY` is provided, the secret key can contain any characters.
- Requires admin authentication.

### Examples

```sneldb
CREATE USER api_client
```

Creates a user named `api_client` and returns a randomly generated secret key.

```sneldb
CREATE USER "service-account" WITH KEY "my_custom_secret_key_12345"
```

Creates a user with a custom secret key.

```sneldb
CREATE USER monitoring_service WITH KEY monitoring_key_2024
```

Creates a user with a word-based secret key (no quotes needed for simple keys).

### Behavior

- Validates the user ID format before creation.
- Checks if the user already exists (returns error if duplicate).
- Stores the user in the protected auth store (backed by `AuthWalStorage`, a dedicated encrypted WAL file).
- Caches the user credentials in memory for fast authentication lookups (auth checks are O(1)).
- Returns the secret key in the response (only shown once during creation).

### Response Format

```
200 OK
User 'api_client' created
Secret key: a1b2c3d4e5f6...
```

### Errors

- `Invalid user ID format`: User ID contains invalid characters or is empty.
- `User already exists: <user_id>`: A user with this ID already exists.

## REVOKE KEY

### Purpose

Revoke a user's authentication key by marking it as inactive. The user will no longer be able to authenticate commands, but their user record remains in the system.

### Form

```sneldb
REVOKE KEY <user_id:WORD or STRING>
```

### Examples

```sneldb
REVOKE KEY api_client
```

Revokes access for the `api_client` user.

```sneldb
REVOKE KEY "service-account"
```

Revokes access for a user with a hyphenated name (quotes required).

### Behavior

- Marks the user's key as inactive in both the database and in-memory cache.
- Previously authenticated connections may continue to work until they disconnect.
- The user record remains in the system for audit purposes.
- To restore access, you must create a new user with a different user ID (or implement key rotation in a future version).
- Requires admin authentication.

### Response Format

```
200 OK
Key revoked for user 'api_client'
```

### Errors

- `User not found: <user_id>`: No user exists with the specified user ID.

## LIST USERS

### Purpose

List all registered users and their current status (active or inactive).

### Form

```sneldb
LIST USERS
```

### Examples

```sneldb
LIST USERS
```

### Behavior

- Returns all users registered in the system.
- Shows each user's ID and active status.
- Does not return secret keys (for security reasons).
- Results are returned from the in-memory cache for fast access.
- Requires admin authentication.

## Auth Storage and Durability

- **Isolation:** User data is stored in a dedicated encrypted WAL file (`AuthWalStorage`) and is not queryable through regular commands. The HTTP dispatcher rejects any `__system_*` context from user input to prevent access to system contexts.
- **Hot path:** Authentication and authorization are served from in-memory caches (`UserCache`, `PermissionCache`) for O(1) checks.
- **Durability:** Mutations (create/revoke/grant/revoke permissions) append to a dedicated secured WAL (`.swal`). Frames are binary, length-prefixed, CRC-checked, encrypted with ChaCha20Poly1305, and versioned with the standard storage header. Corrupted frames are skipped during replay instead of failing startup.
- **Replay:** On startup, the auth WAL is replayed with "latest timestamp wins" semantics to rebuild the caches. The WAL is small because auth mutations are rare compared to data events.
- **Configurable fsync cadence:** The auth WAL flushes each write and fsyncs periodically; you can tune fsync frequency via the WAL settings (or override per storage instance) to balance throughput and durability.
- **Encryption:** Auth WAL payloads are encrypted (AEAD). Provide a 32-byte master key via `SNELDB_AUTH_WAL_KEY` (64-char hex). If unset, a key is derived from the server `auth_token`.

### Response Format

```
200 OK
api_client: active
service-account: active
old_client: inactive
```

If no users exist:

```
200 OK
No users found
```

### Notes

- Secret keys are never returned by this command.
- The list includes both active and inactive users.
- Results are ordered by user ID (implementation-dependent).
- Requires admin authentication.

## GRANT

### Purpose

Grant read and/or write permissions to a user for specific event types. Permissions control which users can query (read) or store (write) events of a given type.

### Form

```sneldb
GRANT <permissions:READ[,WRITE] or WRITE[,READ]> ON <event_type:WORD or STRING>[,<event_type:WORD or STRING>...] TO <user_id:WORD or STRING>
```

### Constraints

- Permissions must be `READ`, `WRITE`, or both (`READ,WRITE` or `WRITE,READ`).
- Event types must be defined using the `DEFINE` command before permissions can be granted.
- Multiple event types can be specified, separated by commas.
- Only admin users can grant permissions.

### Examples

```sneldb
GRANT READ ON orders TO api_client
```

Grants read-only access to the `orders` event type for `api_client`.

```sneldb
GRANT WRITE ON orders TO api_client
```

Grants write-only access to the `orders` event type for `api_client`.

```sneldb
GRANT READ, WRITE ON orders TO api_client
```

Grants both read and write access to the `orders` event type for `api_client`.

```sneldb
GRANT READ, WRITE ON orders, products TO api_client
```

Grants read and write access to both `orders` and `products` event types for `api_client`.

### Behavior

- Validates that the event type exists in the schema registry.
- Merges with existing permissions (grant adds permissions, doesn't remove existing ones).
- Updates permissions in both the database and in-memory cache for fast lookups.
- Permissions take effect immediately for new commands.

### Response Format

```
200 OK
Permissions granted to user 'api_client'
```

### Errors

- `Authentication required`: No user ID provided or authentication failed.
- `Only admin users can manage permissions`: The authenticated user is not an admin.
- `Invalid permission: <perm>`. Must be 'read' or 'write'`: Invalid permission name specified.
- `No schema defined for event type '<event_type>'`: The event type must be defined before permissions can be granted.

## REVOKE (Permissions)

### Purpose

Revoke read and/or write permissions from a user for specific event types. If no permissions are specified, all permissions for the event types are revoked.

### Form

```sneldb
REVOKE [<permissions:READ[,WRITE] or WRITE[,READ]>] ON <event_type:WORD or STRING>[,<event_type:WORD or STRING>...] FROM <user_id:WORD or STRING>
```

### Constraints

- Permissions are optional. If omitted, all permissions for the specified event types are revoked.
- If permissions are specified, only those permissions are revoked (e.g., `REVOKE WRITE` only revokes write permission, leaving read permission intact).
- Multiple event types can be specified, separated by commas.
- Only admin users can revoke permissions.

### Examples

```sneldb
REVOKE READ ON orders FROM api_client
```

Revokes read permission for the `orders` event type from `api_client`, leaving write permission intact if it exists.

```sneldb
REVOKE WRITE ON orders FROM api_client
```

Revokes write permission for the `orders` event type from `api_client`, leaving read permission intact if it exists.

```sneldb
REVOKE READ, WRITE ON orders FROM api_client
```

Revokes both read and write permissions for the `orders` event type from `api_client`.

```sneldb
REVOKE ON orders FROM api_client
```

Revokes all permissions (both read and write) for the `orders` event type from `api_client`.

```sneldb
REVOKE ON orders, products FROM api_client
```

Revokes all permissions for both `orders` and `products` event types from `api_client`.

### Behavior

- Revokes specified permissions for the given event types.
- If all permissions are revoked for an event type, the permission entry is removed entirely.
- Updates permissions in both the database and in-memory cache.
- Changes take effect immediately for new commands.

### Response Format

```
200 OK
Permissions revoked from user 'api_client'
```

### Errors

- `Authentication required`: No user ID provided or authentication failed.
- `Only admin users can manage permissions`: The authenticated user is not an admin.
- `Invalid permission: <perm>`. Must be 'read' or 'write'`: Invalid permission name specified.

## SHOW PERMISSIONS

### Purpose

Display all permissions granted to a specific user, showing which event types they can read and/or write.

### Form

```sneldb
SHOW PERMISSIONS FOR <user_id:WORD or STRING>
```

### Examples

```sneldb
SHOW PERMISSIONS FOR api_client
```

Shows all permissions for `api_client`.

```sneldb
SHOW PERMISSIONS FOR "service-account"
```

Shows all permissions for a user with a hyphenated name.

### Behavior

- Returns all permissions for the specified user.
- Shows each event type and the permissions (read, write, or both).
- Results are returned from the in-memory cache for fast access.
- Requires admin authentication.

### Response Format

```
200 OK
Permissions for user 'api_client':
  orders: read, write
  products: read
  users: write
```

If the user has no permissions:

```
200 OK
Permissions for user 'api_client':
  (has no permissions)
```

### Errors

- `Authentication required`: No user ID provided or authentication failed.
- `Only admin users can manage permissions`: The authenticated user is not an admin.
- `Failed to show permissions`: Internal error retrieving permissions.

## Authentication Flow

### Initial Setup

1. **Create a user:**

   ```sneldb
   CREATE USER my_client
   ```

   Save the returned secret key securely.

2. **Authenticate (TCP/UNIX/WebSocket):**

   **Option A: Session Token (Recommended for high-throughput):**

   ```sneldb
   AUTH my_client:<signature>
   ```

   Where `<signature>` = `HMAC-SHA256(secret_key, "my_client")`

   Response:

   ```
   OK TOKEN <session_token>
   ```

   Then use the token for subsequent commands:

   ```sneldb
   STORE order_created FOR user-123 PAYLOAD {"id": 456} TOKEN <session_token>
   ```

   **Option B: Connection-scoped authentication:**

   ```sneldb
   AUTH my_client:<signature>
   ```

   Then send signed commands:

   ```sneldb
   <signature>:STORE order_created FOR user-123 PAYLOAD {"id": 456}
   ```

   Where `<signature>` = `HMAC-SHA256(secret_key, "STORE order_created FOR user-123 PAYLOAD {\"id\": 456}")`

   **Option C: Inline format (per-command):**

   ```sneldb
   my_client:<signature>:STORE order_created FOR user-123 PAYLOAD {"id": 456}
   ```

   Where `<signature>` = `HMAC-SHA256(secret_key, "STORE order_created FOR user-123 PAYLOAD {\"id\": 456}")`

   **Note:** For WebSocket connections, send these commands as text messages over the WebSocket connection.

### HTTP Authentication

For HTTP requests, include authentication headers:

```
POST /command
X-Auth-User: my_client
X-Auth-Signature: <signature>
Content-Type: application/json

STORE order_created FOR user-123 PAYLOAD {"id": 456}
```

Where `<signature>` = `HMAC-SHA256(secret_key, "STORE order_created FOR user-123 PAYLOAD {\"id\": 456}")`

## Security Considerations

- **Secret keys are sensitive**: Store them securely and never log them.
- **Session tokens are sensitive**: Session tokens provide access to the system and should be protected. Treat them like passwords:
  - Never log tokens or expose them in error messages
  - Use secure channels (TLS/SSL) when transmitting tokens over the network
  - Tokens are stored in-memory only and are lost on server restart (this is a security feature, not a bug)
  - Tokens expire automatically after the configured time (default: 5 minutes)
- **Key rotation**: Currently, revoking a key requires creating a new user. Future versions will support key rotation.
- **Token revocation**: There is no user-facing command to revoke session tokens. Tokens expire automatically, but cannot be manually revoked before expiration.
- **User enumeration**: Error messages may reveal whether a user exists. This is a known limitation.
- **Rate limiting**: Not currently implemented. Consider implementing rate limiting at the network layer.
- **Key storage**: Secret keys are stored in plaintext in SnelDB's internal storage. Ensure proper access controls on the database files.
- **Token storage**: Session tokens are stored in-memory only (not persisted to disk), which means they are lost on server restart but also cannot be recovered from disk if the server is compromised.

## Critical Issues

The following critical security issues need to be addressed:

- [ ] **Secret key exposure**: Secret keys are returned in command responses and may be logged or exposed in network traces.
- [ ] **Session token exposure**: Session tokens are returned in `AUTH` command responses and may be logged or exposed in network traces. Tokens sent with commands (`COMMAND ... TOKEN <token>`) may also be logged.
- [ ] **No token revocation command**: There is no user-facing command to revoke session tokens. Tokens can only be revoked by waiting for expiration or server restart.
- [ ] **User enumeration**: Error messages reveal whether a user exists (`UserNotFound` vs `UserExists`), enabling user enumeration attacks.
- [ ] **Weak constant-time comparison**: The current constant-time comparison implementation has an early return that leaks timing information about signature length.
- [ ] **No rate limiting**: Missing rate limiting allows brute-force attacks on signatures, user creation, and token validation attempts.
- [ ] **Plaintext key storage**: Secret keys are stored in plaintext in the database, exposing all keys if the database is compromised.
- [ ] **Error message leakage**: Detailed error messages reveal internal system details to potential attackers.
- [ ] **No key rotation**: Once compromised, keys cannot be rotated without creating a new user account.
- [ ] **AUTH command signature verification**: The AUTH command signature verification may not match the documented format.
- [ ] **No input length limits**: Missing input length validation allows potential denial-of-service attacks via oversized inputs.
- [ ] **Token validation timing**: Token validation uses hash lookup (O(1)), but error messages may leak information about token existence.

## Permissions and Access Control

SnelDB implements fine-grained access control at the event type level. Users can be granted read and/or write permissions for specific event types:

- **Read permission**: Allows users to query events of the specified event type.
- **Write permission**: Allows users to store events of the specified event type.
- **Admin role**: Users with the admin role have full access to all event types and can manage users and permissions.

Permissions are checked at command execution time:

- `STORE` commands require write permission for the event type.
- `QUERY` commands require read permission for the event type.
- `DEFINE` commands require admin privileges.

Permissions take effect immediately when granted or revoked. Changes apply to new commands; commands already in progress are not affected.

### Admin Users

Admin users are created with the `admin` role. They have full system access and can:

- Create and manage users
- Grant and revoke permissions
- Define event schemas
- Access all event types regardless of permissions

The initial admin user can be configured via the `initial_admin_user` and `initial_admin_key` configuration options, which automatically creates an admin user on first startup if no users exist.

## Future Work

The following improvements are planned for user management:

- **Key rotation**: Allow users to rotate their secret keys without creating a new user account.
- **Key expiration**: Support time-based key expiration and automatic rotation.
- **Audit logging**: Log all authentication attempts, user creation, key revocation, and permission changes for security auditing.
- **Rate limiting**: Implement per-user rate limiting to prevent abuse and brute-force attacks.
- **Key encryption at rest**: Encrypt secret keys in the database using a master encryption key.
- **Multi-factor authentication**: Support additional authentication factors beyond HMAC signatures.
- **User metadata**: Store additional user information (email, description, created date, last access date).
- **Bulk operations**: Support creating or revoking multiple users in a single command.
- **Key strength validation**: Enforce minimum key length and complexity requirements.
- **Session management**: Track active sessions and allow session invalidation. Add a command to revoke session tokens.
- **Token security improvements**: Implement token rotation, token binding to IP addresses, and token refresh mechanisms.
- **Password reset flow**: Implement secure password reset mechanisms for user accounts.
- **User groups**: Organize users into groups for easier management and permission assignment.
