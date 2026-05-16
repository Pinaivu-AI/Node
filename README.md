# pinaivu-node

GPU provider daemon for the Pinaivu inference marketplace.

The node joins the coordinator's libp2p mesh, bids on inference auctions,
runs jobs against a local LLM (Ollama by default), and reports a signed
`CompletionAck` back to the coordinator so the request earns a routing
receipt.

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

## Client smoke

```bash
RESP=$(curl -s -X POST http://localhost:4000/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{"model":"llama3","messages":[{"role":"user","content":"what is 1+1?"}],"client_pubkey_hex":"'$(printf '01%.0s' {1..32})'"}')
NODE_URL=$(echo "$RESP" | jq -r .node_url)
TOKEN=$(echo "$RESP" | jq -c .dispatch_token)
REQ=$(echo "$RESP" | jq -r .request_id)

curl -s -X POST "$NODE_URL/v1/inference" \
  -H 'content-type: application/json' \
  -d "{\"prompt\":\"what is 1+1?\",\"dispatch_token\":$TOKEN}" | jq .

sleep 1
curl -s "http://localhost:4000/v1/proofs/$REQ" | jq .
```
