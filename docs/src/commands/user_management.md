# User Management

## Purpose

SnelDB provides authentication and authorization through HMAC-based signatures. User management commands allow you to create users, revoke their access keys, and list all registered users.

All commands (except user management commands themselves) require authentication via HMAC-SHA256 signatures. This ensures that only authorized clients can store, query, or modify data.

## Authentication Overview

SnelDB uses HMAC-SHA256 for message authentication. Each user has a secret key that is used to sign commands. The signature proves that the command was issued by someone who knows the secret key.

### Authentication Formats

**TCP/UNIX (after AUTH command):**

```sneldb
AUTH user_id:signature
signature:STORE event_type FOR context_id PAYLOAD {...}
```

**TCP/UNIX (inline format):**

```sneldb
user_id:signature:STORE event_type FOR context_id PAYLOAD {...}
```

**HTTP (header-based):**

```
X-Auth-User: user_id
X-Auth-Signature: signature
```

The signature is computed as: `HMAC-SHA256(secret_key, message)` where `message` is the command string being executed.

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
- Stores the user in SnelDB's internal `__auth_user` event type.
- Caches the user credentials in memory for fast authentication lookups.
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

## Authentication Flow

### Initial Setup

1. **Create a user:**

   ```sneldb
   CREATE USER my_client
   ```

   Save the returned secret key securely.

2. **Authenticate (TCP/UNIX):**

   ```sneldb
   AUTH my_client:<signature>
   ```

   Where `<signature>` = `HMAC-SHA256(secret_key, "my_client")`

3. **Send authenticated commands:**
   ```sneldb
   <signature>:STORE order_created FOR user-123 PAYLOAD {"id": 456}
   ```
   Where `<signature>` = `HMAC-SHA256(secret_key, "STORE order_created FOR user-123 PAYLOAD {\"id\": 456}")`

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
- **Key rotation**: Currently, revoking a key requires creating a new user. Future versions will support key rotation.
- **User enumeration**: Error messages may reveal whether a user exists. This is a known limitation.
- **Rate limiting**: Not currently implemented. Consider implementing rate limiting at the network layer.
- **Key storage**: Secret keys are stored in plaintext in SnelDB's internal storage. Ensure proper access controls on the database files.

## Critical Issues

The following critical security issues need to be addressed:

- [ ] **Authentication bypass**: User management commands (`CREATE USER`, `REVOKE KEY`, `LIST USERS`) currently do not require authentication, allowing anyone to create users or revoke keys.
- [ ] **Secret key exposure**: Secret keys are returned in command responses and may be logged or exposed in network traces.
- [ ] **User enumeration**: Error messages reveal whether a user exists (`UserNotFound` vs `UserExists`), enabling user enumeration attacks.
- [ ] **Weak constant-time comparison**: The current constant-time comparison implementation has an early return that leaks timing information about signature length.
- [ ] **No rate limiting**: Missing rate limiting allows brute-force attacks on signatures and user creation.
- [ ] **Plaintext key storage**: Secret keys are stored in plaintext in the database, exposing all keys if the database is compromised.
- [ ] **Error message leakage**: Detailed error messages reveal internal system details to potential attackers.
- [ ] **No key rotation**: Once compromised, keys cannot be rotated without creating a new user account.
- [ ] **AUTH command signature verification**: The AUTH command signature verification may not match the documented format.
- [ ] **No input length limits**: Missing input length validation allows potential denial-of-service attacks via oversized inputs.

## Future Work

The following improvements are planned for user management:

- **Key rotation**: Allow users to rotate their secret keys without creating a new user account.
- **Key expiration**: Support time-based key expiration and automatic rotation.
- **Role-based access control**: Define roles and assign permissions to users (e.g., read-only, write-only, admin).
- **Audit logging**: Log all authentication attempts, user creation, and key revocation events for security auditing.
- **Rate limiting**: Implement per-user rate limiting to prevent abuse and brute-force attacks.
- **Key encryption at rest**: Encrypt secret keys in the database using a master encryption key.
- **Multi-factor authentication**: Support additional authentication factors beyond HMAC signatures.
- **User metadata**: Store additional user information (email, description, created date, last access date).
- **Bulk operations**: Support creating or revoking multiple users in a single command.
- **Key strength validation**: Enforce minimum key length and complexity requirements.
- **Session management**: Track active sessions and allow session invalidation.
- **Password reset flow**: Implement secure password reset mechanisms for user accounts.
- **API key scoping**: Allow keys to be scoped to specific event types or contexts.
- **User groups**: Organize users into groups for easier management and permission assignment.
