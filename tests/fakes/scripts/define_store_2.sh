#!/bin/bash

  echo 'DEFINE subscription FIELDS { "id": "int", "plan": "string" }'  | socat - UNIX-CONNECT:/tmp/sneldb.sock
  echo 'STORE subscription FOR ctx1 PAYLOAD {"id": 1, "plan": "free"}' | socat - UNIX-CONNECT:/tmp/sneldb.sock
  echo 'STORE subscription FOR ctx2 PAYLOAD {"id": 2, "plan": "pro"}' | socat - UNIX-CONNECT:/tmp/sneldb.sock
