# User Management

## Purpose

SnelDB provides authentication and authorization through HMAC-based signatures and session tokens. User management commands allow you to create users, revoke their access keys, list all registered users, and manage fine-grained permissions for event types.

SnelDB uses a two-tier access control system:

- **Roles**: Broad privileges (e.g., "can read everything", "can write everything")
- **Permissions**: Fine-grained access per event type (e.g., "can read orders", "can write payments")

**Important:** Permissions override roles, but the behavior depends on what the permission grants or denies. If you have a `read-only` role but are granted write permission on a specific event type, you can write to that event type while still reading from your role. See [Roles and Permissions](#roles-and-permissions) for detailed examples.

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

Create a new user with authentication credentials and optional roles. The user will receive a secret key that can be used to sign commands. Roles provide broad access privileges, while permissions provide fine-grained control per event type.

### Form

```sneldb
CREATE USER <user_id:WORD or STRING> [ WITH KEY <secret_key:STRING> ] [ WITH ROLES [<role:STRING>[,<role:STRING>...]] ]
```

### Constraints

- `<user_id>` must be non-empty and contain only alphanumeric characters, underscores, or hyphens.
- `<user_id>` is case-sensitive (e.g., `user1` ≠ `User1`).
- If `WITH KEY` is omitted, a random 64-character hexadecimal secret key is generated.
- If `WITH KEY` is provided, the secret key can contain any characters.
- `WITH ROLES` is optional. If omitted, the user has no roles (access controlled only by permissions).
- Roles can be specified as string literals (e.g., `"admin"`) or word identifiers (e.g., `admin`).
- Multiple roles can be specified in the array.
- `WITH KEY` and `WITH ROLES` can be specified in any order.
- Requires admin authentication.

### Supported Roles

SnelDB supports the following roles:

- **`admin`**: Full system access. Can read/write all event types and manage users/permissions.
- **`read-only`** or **`viewer`**: Can read all event types, but cannot write. Useful for monitoring and analytics users.
- **`editor`**: Can read and write all event types, but cannot manage users or permissions. Useful for data entry users.
- **`write-only`**: Can write all event types, but cannot read. Useful for data ingestion services.

**Note:** Roles provide broad access, but specific permissions can override role-based access (see [Roles and Permissions](#roles-and-permissions) below).

### Examples

```sneldb
CREATE USER api_client
```

Creates a user named `api_client` with no roles. Access is controlled entirely by permissions.

```sneldb
CREATE USER "service-account" WITH KEY "my_custom_secret_key_12345"
```

Creates a user with a custom secret key and no roles.

```sneldb
CREATE USER monitoring_service WITH KEY monitoring_key_2024 WITH ROLES ["read-only"]
```

Creates a read-only user for monitoring purposes. This user can read all event types but cannot write.

```sneldb
CREATE USER data_entry WITH ROLES ["editor"]
```

Creates an editor user who can read and write all event types.

```sneldb
CREATE USER admin_user WITH ROLES ["admin"]
```

Creates an admin user with full system access.

```sneldb
CREATE USER viewer WITH ROLES ["viewer"]
```

Creates a viewer user (alias for read-only role).

```sneldb
CREATE USER writer WITH ROLES ["write-only"]
```

Creates a write-only user who can write but not read.

```sneldb
CREATE USER multi_role WITH ROLES ["admin", "read-only"]
```

Creates a user with multiple roles. Admin role takes precedence.

```sneldb
CREATE USER api_client WITH KEY "secret" WITH ROLES ["read-only"]
```

Creates a user with both a custom key and a role. Order doesn't matter.

```sneldb
CREATE USER api_client WITH ROLES ["read-only"] WITH KEY "secret"
```

Same as above, with roles and key in different order.

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

## Roles and Permissions

SnelDB implements a two-tier access control system combining **roles** (broad privileges) and **permissions** (fine-grained access per event type).

### Understanding Roles vs Permissions

- **Roles**: Provide broad, organization-wide access privileges (e.g., "can read everything", "can write everything")
- **Permissions**: Provide specific access to individual event types (e.g., "can read orders", "can write payments")

### Access Control Priority

When checking access, SnelDB uses the following priority order:

1. **Admin role** → Full access (highest priority)
2. **Specific permissions** → Override roles (most granular)
3. **Roles** → Apply when no specific permissions exist (broader access)
4. **Deny** → If no permissions and no roles

**Key Principle:** Permissions override roles, but only for the specific permission type being checked. This allows for flexible access control where roles provide defaults and permissions provide exceptions.

#### How Permission Override Works

The permission override logic works differently for READ and WRITE checks:

**For READ access:**

- If a permission set **grants READ** (`read=true`), use it (permission overrides role)
- If a permission set **denies READ** (`read=false`) but grants WRITE (`write=true`), fall back to role for READ
  - **Rationale:** `GRANT WRITE` is additive—it adds write capability without removing existing read access from roles. If you want write-only access, explicitly `REVOKE READ` first.
- If a permission set **explicitly denies both** (`read=false, write=false`), deny access completely (override role)
- If **no permission set exists**, use role

**For WRITE access:**

- If a permission set **exists for the event type**, it completely overrides the role (both granting and denying)
- If **no permission set exists**, use role

This design ensures that:

- Granting WRITE permission doesn't remove READ access from roles
- Revoking WRITE permission explicitly denies WRITE even if role would grant it
- Revoking all permissions creates an explicit denial that overrides roles

### Roles

SnelDB supports the following roles:

| Role                   | Read All Events | Write All Events | Admin Functions |
| ---------------------- | --------------- | ---------------- | --------------- |
| `admin`                | ✅              | ✅               | ✅              |
| `read-only` / `viewer` | ✅              | ❌               | ❌              |
| `editor`               | ✅              | ✅               | ❌              |
| `write-only`           | ❌              | ✅               | ❌              |

**Role Behavior:**

- **Admin**: Full system access. Can manage users, grant permissions, define schemas, and access all event types.
- **Read-only / Viewer**: Can read all event types by default, but cannot write. Useful for monitoring, analytics, or reporting users.
- **Editor**: Can read and write all event types by default, but cannot manage users or permissions. Useful for data entry or ETL processes.
- **Write-only**: Can write all event types by default, but cannot read. Useful for data ingestion services that only need to store events.

### Permissions

Permissions provide fine-grained access control at the event type level:

- **Read permission**: Allows users to query events of the specified event type.
- **Write permission**: Allows users to store events of the specified event type.

Permissions can be granted per event type, allowing precise control over what each user can access.

### How Roles and Permissions Work Together

**Example 1: Read-only role with write permission**

```sneldb
CREATE USER analyst WITH ROLES ["read-only"]
GRANT WRITE ON special_events TO analyst
```

Result:

- ✅ Can read all event types (read-only role provides READ)
- ✅ Can write to `special_events` (permission grants WRITE)
- ❌ Cannot write to other event types (read-only role denies WRITE)

**Why:** The permission grants WRITE, so it overrides the role for WRITE. But since the permission doesn't grant READ (`read=false`), the system falls back to the role for READ access, which the read-only role provides.

**Example 2: Editor role with restrictive permission**

```sneldb
CREATE USER editor_user WITH ROLES ["editor"]
GRANT READ ON sensitive_data TO editor_user
REVOKE WRITE ON sensitive_data FROM editor_user
```

Result:

- ✅ Can read/write most event types (editor role)
- ✅ Can read `sensitive_data` (permission grants READ)
- ❌ Cannot write to `sensitive_data` (permission denies WRITE, overrides role)

**Why:** The permission set exists for `sensitive_data` with `read=true, write=false`. For READ, the permission grants it. For WRITE, the permission explicitly denies it, which overrides the editor role's ability to write.

**Example 3: Write-only role with read permission**

```sneldb
CREATE USER ingester WITH ROLES ["write-only"]
GRANT READ ON status_events TO ingester
```

Result:

- ✅ Can write all event types (write-only role)
- ✅ Can read `status_events` (permission grants READ)
- ❌ Cannot read other event types (write-only role denies READ)

**Why:** The permission grants READ for `status_events`, so it overrides the role. For other event types, no permission exists, so the write-only role applies (can write, cannot read).

**Example 4: Revoking all permissions**

```sneldb
CREATE USER readonly_user WITH ROLES ["read-only"]
GRANT READ, WRITE ON orders TO readonly_user
REVOKE READ, WRITE ON orders FROM readonly_user
```

Result:

- ❌ Cannot read `orders` (explicit denial overrides role)
- ❌ Cannot write `orders` (explicit denial overrides role)
- ✅ Can read other event types (read-only role applies)

**Why:** When all permissions are revoked, a permission set with `read=false, write=false` is created. This explicit denial overrides the role completely for that event type.

**Example 5: No role, permissions only**

```sneldb
CREATE USER api_client
GRANT READ, WRITE ON orders TO api_client
GRANT READ ON products TO api_client
```

Result:

- ✅ Can read/write `orders` (permission)
- ✅ Can read `products` (permission)
- ❌ Cannot access other event types (no role, no permission)

**Why:** Without a role, access is controlled entirely by permissions. No permission means no access.

**Example 6: Permission grants only one type**

```sneldb
CREATE USER readonly_user WITH ROLES ["read-only"]
GRANT WRITE ON events TO readonly_user
```

Result:

- ✅ Can read `events` (role provides READ, permission doesn't grant it so falls back to role)
- ✅ Can write `events` (permission grants WRITE)
- ✅ Can read other event types (read-only role)
- ❌ Cannot write other event types (read-only role)

**Why:** The permission grants WRITE but not READ (`read=false, write=true`). For READ, since the permission doesn't grant it, the system falls back to the role, which provides READ. For WRITE, the permission grants it, overriding the role's denial.

### Permission Checking

Permissions are checked at command execution time:

- `STORE` commands require write permission for the event type (or appropriate role).
- `QUERY` commands require read permission for the event type (or appropriate role).
- `DEFINE` commands require admin role.
- User management commands (`CREATE USER`, `REVOKE KEY`, `LIST USERS`) require admin role.
- Permission management commands (`GRANT`, `REVOKE`, `SHOW PERMISSIONS`) require admin role.

Permissions take effect immediately when granted or revoked. Changes apply to new commands; commands already in progress are not affected.

### Admin Users

Admin users are created with the `admin` role. They have full system access and can:

- Create and manage users
- Grant and revoke permissions
- Define event schemas
- Access all event types regardless of permissions or other roles

The initial admin user can be configured via the `initial_admin_user` and `initial_admin_key` configuration options, which automatically creates an admin user on first startup if no users exist.

### Best Practices

1. **Use roles for broad access patterns**: Assign roles like `read-only` or `editor` when users need consistent access across many event types.

2. **Use permissions for exceptions**: Grant specific permissions when you need to override role-based access for particular event types.

3. **Combine roles and permissions**: Use roles as defaults and permissions as exceptions. For example, give most users a `read-only` role, then grant write permissions only on specific event types they need to modify.

4. **Start restrictive**: Create users without roles initially, then grant specific permissions. Add roles only when users need broader access.

5. **Document access patterns**: Keep track of which users have which roles and permissions to maintain security and compliance.

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
