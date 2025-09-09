#!/bin/bash

  echo 'DEFINE subscription FIELDS { "id": "int", "plan": "string" }'  | socat - UNIX-CONNECT:/tmp/sneldb.sock
  echo 'DEFINE user_signup FIELDS { "user_id": "int", "email": "string", "country": "string" }' | socat - UNIX-CONNECT:/tmp/sneldb.sock
  echo 'STORE subscription FOR ctx2 PAYLOAD {"id": 2, "plan": "free"}' | socat - UNIX-CONNECT:/tmp/sneldb.sock
  echo 'STORE subscription FOR ctx1 PAYLOAD {"id": 1, "plan": "pro"}' | socat - UNIX-CONNECT:/tmp/sneldb.sock
  echo 'STORE subscription FOR ctx3 PAYLOAD {"id": 3, "plan": "premium"}' | socat - UNIX-CONNECT:/tmp/sneldb.sock
  echo 'STORE subscription FOR ctx4 PAYLOAD {"id": 4, "plan": "enterprise"}' | socat - UNIX-CONNECT:/tmp/sneldb.sock
  echo 'STORE user_signup FOR ctx1 PAYLOAD {"user_id": 1, "email": "john@example.com", "country": "NL"}' | socat - UNIX-CONNECT:/tmp/sneldb.sock
  echo 'STORE user_signup FOR ctx2 PAYLOAD {"user_id": 2, "email": "jane@example.com", "country": "DE"}' | socat - UNIX-CONNECT:/tmp/sneldb.sock
  echo 'STORE user_signup FOR ctx4 PAYLOAD {"user_id": 4, "email": "jim@example.com", "country": "FR"}' | socat - UNIX-CONNECT:/tmp/sneldb.sock
  echo 'STORE user_signup FOR ctx3 PAYLOAD {"user_id": 3, "email": "jill@example.com", "country": "ES"}' | socat - UNIX-CONNECT:/tmp/sneldb.sock