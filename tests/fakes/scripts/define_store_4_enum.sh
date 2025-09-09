#!/bin/bash

  echo 'DEFINE subscription FIELDS { "id": "int", "plan": ["free", "pro", "premium", "enterprise"] }'  | socat - UNIX-CONNECT:/tmp/sneldb.sock
  echo 'STORE subscription FOR ctx1 PAYLOAD {"id": 1, "plan": "free"}' | socat - UNIX-CONNECT:/tmp/sneldb.sock
  echo 'STORE subscription FOR ctx2 PAYLOAD {"id": 2, "plan": "pro"}' | socat - UNIX-CONNECT:/tmp/sneldb.sock
  echo 'STORE subscription FOR ctx3 PAYLOAD {"id": 3, "plan": "premium"}' | socat - UNIX-CONNECT:/tmp/sneldb.sock
  echo 'STORE subscription FOR ctx4 PAYLOAD {"id": 4, "plan": "enterprise"}' | socat - UNIX-CONNECT:/tmp/sneldb.sock
  echo 'FLUSH' | socat - UNIX-CONNECT:/tmp/sneldb.sock
