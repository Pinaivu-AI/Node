# pinaivu-node

GPU provider daemon for Pinaivu's decentralized inference network.

The node joins the coordinator's libp2p mesh, bids on inference auctions,
runs jobs against a local LLM (Ollama by default), and reports a signed
`CompletionAck` back to the coordinator so the request earns a routing
receipt. Anyone can run a node and join the mesh; no permission from
Pinaivu is required. This is the genuinely decentralized layer of the
network, alongside Walrus storage — see the [decentralization &
verifiability model](https://docs.pinaivu.com/architecture/decentralization)
for how it fits with the off-chain-verifiable coordinator and the on-chain
Sui contracts.

For stateful sessions the node fetches conversation history from Postgres
and an encrypted Walrus blob, decrypts it with a per-session key the
client supplies, and persists the updated session back to Walrus after
each turn. Stateless one-shot requests are also supported.

## Run

```bash
# 1. Local LLM
ollama serve
ollama pull llama3

# 2. Coordinator (separate repo) — copy its libp2p_addr from its logs
cargo run -p coordinator     # in the coordinator workspace

# 3. Node
cargo run --release -- \
  --coordinator-addr /ip4/127.0.0.1/tcp/<PORT>/p2p/<PEER_ID> \
  --coordinator-http http://127.0.0.1:4000 \
  --listen 127.0.0.1:5000 \
  --ollama-url http://localhost:11434 \
  --model llama3 \
  --price-per-1k-nanox 50
```

Stateful sessions additionally need `DATABASE_URL` set so the node can
fetch and persist conversation history.

## Client smoke

```bash
RESP=$(curl -s -X POST http://localhost:4000/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{"model":"llama3","messages":[{"role":"user","content":"what is 1+1?"}],"client_pubkey_hex":"'$(printf '01%.0s' {1..32})'"}')
NODE_URL=$(echo "$RESP" | jq -r .node_url)
TOKEN=$(echo "$RESP" | jq -c .dispatch_token)
REQ=$(echo "$RESP" | jq -r .request_id)
SESSION_ID=$(echo "$RESP" | jq -r .session_id)

curl -s -X POST "$NODE_URL/v1/inference" \
  -H 'content-type: application/json' \
  -d "{\"new_user_message\":\"what is 1+1?\",\"dispatch_token\":$TOKEN,\"session_id\":\"$SESSION_ID\"}" | jq .

sleep 1
curl -s "http://localhost:4000/v1/proofs/$REQ" | jq .
```

Add `"session_key": "<base64 32 bytes>"` to the `/v1/inference` body to
make the turn stateful: the node will fetch and persist conversation
history for that `session_id`, encrypted with that key.
