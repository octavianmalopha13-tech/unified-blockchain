package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ---------- Blockchain Core ----------
type Block struct {
	Index     int    `json:"index"`
	Data      string `json:"data"`
	Timestamp int64  `json:"timestamp"`
	PrevHash  string `json:"prevHash"`
	Hash      string `json:"hash"`
	Difficulty string `json:"difficulty"`
}

type Blockchain struct {
	Chain []Block
}

var bc *Blockchain
var mempool []string
var mu sync.Mutex
var chainMu sync.Mutex
var stopMiner chan struct{}
var peers []*websocket.Conn
var peersMu sync.Mutex
var difficultyPrefix = "00"
var difficultyMu sync.Mutex

// ---------- Wallet ----------
var wallets = make(map[string]int)
var walletsMu sync.Mutex

// ---------- Chat ----------
type ChatMessage struct {
	Room     string `json:"room"`
	Username string `json:"username"`
	Text     string `json:"text"`
	Time     string `json:"time"`
}
var chatClients = make(map[*websocket.Conn]string) // conn -> room
var chatClientsMu sync.Mutex
var chatBroadcast = make(chan ChatMessage)

// ---------- KV Cache (optional) ----------
var kvCache = make(map[string]string)
var kvCacheMu sync.Mutex

// ---------- WebSocket Upgrader ----------
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// ---------- Blockchain Helpers ----------
func calculateHash(b Block) string {
	data := strconv.Itoa(b.Index) + strconv.FormatInt(b.Timestamp, 10) + b.Data + b.PrevHash
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", hash[:])
}

func isValidHash(hash string) bool {
	difficultyMu.Lock()
	prefix := difficultyPrefix
	difficultyMu.Unlock()
	return strings.HasPrefix(hash, prefix)
}

func NewBlock(index int, data string, prevHash string) Block {
	difficultyMu.Lock()
	diff := difficultyPrefix
	difficultyMu.Unlock()

	newBlock := Block{
		Index:      index,
		Timestamp:  time.Now().Unix(),
		Data:       data,
		PrevHash:   prevHash,
		Hash:       "",
		Difficulty: diff,
	}
	for {
		newBlock.Hash = calculateHash(newBlock)
		if isValidHash(newBlock.Hash) {
			break
		}
		newBlock.Timestamp++
	}
	return newBlock
}

func (bc *Blockchain) AddBlock(data string) (Block, error) {
	last := bc.Chain[len(bc.Chain)-1]
	newBlock := NewBlock(last.Index+1, data, last.Hash)

	if newBlock.PrevHash != last.Hash {
		return newBlock, fmt.Errorf("Invalid block: Hash mismatch")
	}
	if !isValidHash(newBlock.Hash) {
		return newBlock, fmt.Errorf("Invalid block: Hash doesn't start with %s", difficultyPrefix)
	}

	chainMu.Lock()
	bc.Chain = append(bc.Chain, newBlock)
	chainMu.Unlock()

	return newBlock, nil
}

func (bc *Blockchain) AddTransaction(data string) error {
	mu.Lock()
	mempool = append(mempool, data)
	mu.Unlock()
	return nil
}

func (bc *Blockchain) ValidateChain() bool {
	chainMu.Lock()
	defer chainMu.Unlock()

	if len(bc.Chain) == 0 {
		return false
	}
	// Check genesis with its own difficulty
	if !strings.HasPrefix(bc.Chain[0].Hash, bc.Chain[0].Difficulty) {
		return false
	}
	for i := 1; i < len(bc.Chain); i++ {
		curr := bc.Chain[i]
		prev := bc.Chain[i-1]
		if calculateHash(curr) != curr.Hash {
			return false
		}
		if curr.PrevHash != prev.Hash {
			return false
		}
		if !strings.HasPrefix(curr.Hash, curr.Difficulty) {
			return false
		}
	}
	return true
}

func (bc *Blockchain) PrintChain() {
	chainMu.Lock()
	defer chainMu.Unlock()
	for _, b := range bc.Chain {
		fmt.Printf("Index: %d, Data: %s, Hash: %s, Prev: %s\n", b.Index, b.Data, b.Hash, b.PrevHash)
	}
}

// ---------- Persistence ----------
func saveState() {
	mu.Lock()
	mempoolData, _ := json.MarshalIndent(mempool, "", "  ")
	mu.Unlock()

	chainMu.Lock()
	chainData, err := json.MarshalIndent(bc.Chain, "", "  ")
	chainMu.Unlock()
	if err == nil {
		os.WriteFile("blockchain.json", chainData, 0644)
	}
	os.WriteFile("mempool.json", mempoolData, 0644)
	log.Println("✅ State saved")
}

func loadState() (*Blockchain, []string) {
	chainData, err := os.ReadFile("blockchain.json")
	if err == nil {
		var chain []Block
		if json.Unmarshal(chainData, &chain) == nil && len(chain) > 0 {
			mempoolData, _ := os.ReadFile("mempool.json")
			var loadedMempool []string
			json.Unmarshal(mempoolData, &loadedMempool)
			log.Printf("✅ Loaded %d blocks", len(chain))
			return &Blockchain{Chain: chain}, loadedMempool
		}
	}
	log.Println("🆕 No saved state, creating genesis")
	genesis := Block{Index: 0, Timestamp: time.Now().Unix(), Data: "Genesis", PrevHash: "", Hash: "", Difficulty: "00"}
	for {
		genesis.Hash = calculateHash(genesis)
		if isValidHash(genesis.Hash) {
			break
		}
		genesis.Timestamp++
	}
	return &Blockchain{Chain: []Block{genesis}}, []string{}
}

// ---------- Dynamic Difficulty ----------
func adjustDifficulty() {
	chainMu.Lock()
	if len(bc.Chain) < 10 {
		chainMu.Unlock()
		return
	}
	start := len(bc.Chain) - 10
	totalTime := bc.Chain[len(bc.Chain)-1].Timestamp - bc.Chain[start].Timestamp
	avgTime := totalTime / 10
	chainMu.Unlock()

	difficultyMu.Lock()
	defer difficultyMu.Unlock()
	if avgTime < 5 {
		difficultyPrefix = difficultyPrefix + "0"
		log.Printf("⛏️ Difficulty increased to %s (avg %ds)", difficultyPrefix, avgTime)
	} else if avgTime > 15 && len(difficultyPrefix) > 1 {
		difficultyPrefix = difficultyPrefix[:len(difficultyPrefix)-1]
		log.Printf("⛏️ Difficulty decreased to %s (avg %ds)", difficultyPrefix, avgTime)
	}
}

// ---------- Miner ----------
func mineBlocks() {
	for {
		select {
		case <-stopMiner:
			log.Println("⏹️ Miner stopped")
			return
		default:
			mu.Lock()
			if len(mempool) == 0 {
				mu.Unlock()
				time.Sleep(1 * time.Second)
				continue
			}
			batch := []string{}
			for i := 0; i < 5 && len(mempool) > 0; i++ {
				batch = append(batch, mempool[0])
				mempool = mempool[1:]
			}
			mu.Unlock()

			data := strings.Join(batch, "|")
			newBlock, err := bc.AddBlock(data)
			if err == nil {
				adjustDifficulty()
				broadcastBlock(newBlock)
				broadcastNewBlock(newBlock)
				log.Printf("⛏️ Mined: %s", data)
			} else {
				log.Printf("❌ Failed to mine: %v", err)
			}
		}
	}
}

// ---------- P2P ----------
func broadcastBlock(block Block) {
	msg := struct {
		Type string `json:"type"`
		Data Block  `json:"data"`
	}{Type: "new_block", Data: block}
	peersMu.Lock()
	defer peersMu.Unlock()
	for _, p := range peers {
		p.WriteJSON(msg)
	}
}

func broadcastNewBlock(block Block) {
	// WebSocket UI
	chatClientsMu.Lock()
	for conn := range chatClients {
		conn.WriteJSON(block)
	}
	chatClientsMu.Unlock()
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	peersMu.Lock()
	peers = append(peers, conn)
	peersMu.Unlock()
	go func() {
		defer conn.Close()
		for {
			var msg map[string]interface{}
			err := conn.ReadJSON(&msg)
			if err != nil {
				peersMu.Lock()
				for i, p := range peers {
					if p == conn {
						peers = append(peers[:i], peers[i+1:]...)
						break
					}
				}
				peersMu.Unlock()
				return
			}
		}
	}()
}

// ---------- HTTP Handlers ----------
func handleGetBlocks(w http.ResponseWriter, r *http.Request) {
	chainMu.Lock()
	defer chainMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(bc.Chain)
}

func handlePostBlock(w http.ResponseWriter, r *http.Request) {
	var req struct{ Data string }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	bc.AddTransaction(req.Data)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "tx added", "data": req.Data})
}

func handleGetBlockByIndex(w http.ResponseWriter, r *http.Request) {
	indexStr := r.PathValue("index")
	index, err := strconv.Atoi(indexStr)
	if err != nil || index < 0 || index >= len(bc.Chain) {
		http.Error(w, "Invalid index", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(bc.Chain[index])
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Node running fine"))
}

func handleGetMempool(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(mempool)
}

func handleGetPeers(w http.ResponseWriter, r *http.Request) {
	peersMu.Lock()
	defer peersMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"count": len(peers)})
}

// ---------- Wallet Handlers ----------
func handleCreateWallet(w http.ResponseWriter, r *http.Request) {
	walletsMu.Lock()
	defer walletsMu.Unlock()
	addr := fmt.Sprintf("0x%x", time.Now().UnixNano())
	for len(addr) < 20 {
		addr += "0"
	}
	addr = addr[:20]
	wallets[addr] = 100
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"address": addr,
		"balance": 100,
		"status":  "wallet created",
	})
}

func handleGetBalance(w http.ResponseWriter, r *http.Request) {
	addr := r.URL.Query().Get("address")
	if addr == "" {
		http.Error(w, "Address required", http.StatusBadRequest)
		return
	}
	walletsMu.Lock()
	balance := wallets[addr]
	walletsMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"address": addr,
		"balance": balance,
	})
}

func handleSendCoins(w http.ResponseWriter, r *http.Request) {
	var req struct {
		From   string `json:"from"`
		To     string `json:"to"`
		Amount int    `json:"amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	walletsMu.Lock()
	defer walletsMu.Unlock()
	if _, ok := wallets[req.From]; !ok {
		http.Error(w, "Sender not found", http.StatusBadRequest)
		return
	}
	if _, ok := wallets[req.To]; !ok {
		http.Error(w, "Recipient not found", http.StatusBadRequest)
		return
	}
	if wallets[req.From] < req.Amount {
		http.Error(w, "Insufficient balance", http.StatusBadRequest)
		return
	}
	wallets[req.From] -= req.Amount
	wallets[req.To] += req.Amount
	txData := fmt.Sprintf("%s->%s:%d", req.From, req.To, req.Amount)
	bc.AddTransaction(txData)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "sent",
		"from":   req.From,
		"to":     req.To,
		"amount": req.Amount,
	})
}

func handleWalletHistory(w http.ResponseWriter, r *http.Request) {
	addr := r.URL.Query().Get("address")
	if addr == "" {
		http.Error(w, "Address required", http.StatusBadRequest)
		return
	}
	chainMu.Lock()
	defer chainMu.Unlock()
	var txs []string
	for _, b := range bc.Chain {
		if strings.Contains(b.Data, addr) {
			txs = append(txs, b.Data)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"address":      addr,
		"transactions": txs,
	})
}

// ---------- KV Handlers (on-chain) ----------
func handleKVSet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Key == "" {
		http.Error(w, "Key required", http.StatusBadRequest)
		return
	}
	// Store on blockchain
	txData := fmt.Sprintf("KV:SET:%s=%s", req.Key, req.Value)
	bc.AddTransaction(txData)

	// Also update cache
	kvCacheMu.Lock()
	kvCache[req.Key] = req.Value
	kvCacheMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "stored on blockchain",
		"key":    req.Key,
		"value":  req.Value,
	})
}

func handleKVGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key required", http.StatusBadRequest)
		return
	}
	// Check cache first
	kvCacheMu.Lock()
	if val, ok := kvCache[key]; ok {
		kvCacheMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"key": key, "value": val})
		return
	}
	kvCacheMu.Unlock()

	// Scan blockchain
	chainMu.Lock()
	defer chainMu.Unlock()
	var value string
	found := false
	for i := len(bc.Chain) - 1; i >= 0; i-- {
		block := bc.Chain[i]
		if strings.Contains(block.Data, "KV:SET:"+key+"=") {
			parts := strings.SplitN(block.Data, "=", 2)
			if len(parts) == 2 {
				value = parts[1]
				found = true
				break
			}
		}
	}
	if !found {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	// Update cache
	kvCacheMu.Lock()
	kvCache[key] = value
	kvCacheMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": value})
}

// ---------- Chat Handlers ----------
func handleChatWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	username := r.URL.Query().Get("username")
	if username == "" {
		username = "Anonymous"
	}
	room := r.URL.Query().Get("room")
	if room == "" {
		room = "general"
	}

	chatClientsMu.Lock()
	chatClients[conn] = room
	chatClientsMu.Unlock()

	// Send history
	sendChatHistory(conn, room)

	// Broadcast join
	chatBroadcast <- ChatMessage{
		Room:     room,
		Username: username,
		Text:     "joined the chat",
		Time:     time.Now().Format("15:04:05"),
	}

	for {
		var msg ChatMessage
		err := conn.ReadJSON(&msg)
		if err != nil {
			chatClientsMu.Lock()
			delete(chatClients, conn)
			chatClientsMu.Unlock()
			chatBroadcast <- ChatMessage{
				Room:     room,
				Username: username,
				Text:     "left the chat",
				Time:     time.Now().Format("15:04:05"),
			}
			break
		}
		if msg.Username == "" {
			msg.Username = username
		}
		if msg.Room == "" {
			msg.Room = room
		}
		msg.Time = time.Now().Format("15:04:05")

		// Store on blockchain
		txData := fmt.Sprintf("CHAT:%s:%s:%s", msg.Room, msg.Username, msg.Text)
		bc.AddTransaction(txData)

		// Broadcast to room
		chatBroadcast <- msg
	}
}

func sendChatHistory(conn *websocket.Conn, room string) {
	chainMu.Lock()
	defer chainMu.Unlock()
	var messages []ChatMessage
	for _, b := range bc.Chain {
		if strings.HasPrefix(b.Data, "CHAT:"+room+":") {
			parts := strings.SplitN(b.Data, ":", 4)
			if len(parts) == 4 {
				messages = append(messages, ChatMessage{
					Room:     room,
					Username: parts[2],
					Text:     parts[3],
					Time:     time.Unix(b.Timestamp, 0).Format("15:04:05"),
				})
			}
		}
	}
	if len(messages) > 50 {
		messages = messages[len(messages)-50:]
	}
	conn.WriteJSON(map[string]interface{}{
		"type": "history",
		"data": messages,
	})
}

func broadcastChatMessages() {
	for {
		msg := <-chatBroadcast
		chatClientsMu.Lock()
		for conn, room := range chatClients {
			if room == msg.Room {
				conn.WriteJSON(msg)
			}
		}
		chatClientsMu.Unlock()
	}
}

func handleChatUI(w http.ResponseWriter, r *http.Request) {
	html := `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8"><title>Unified Chat</title>
<style>
body { font-family: sans-serif; background: #0d1117; color: #c9d1d9; margin: 0; padding: 20px; }
.chat-container { max-width: 800px; margin: 0 auto; }
h1 { color: #58a6ff; }
.header { display: flex; gap: 10px; margin-bottom: 10px; flex-wrap: wrap; }
.header input, .header select { padding: 8px; border-radius: 4px; border: 1px solid #30363d; background: #21262d; color: #c9d1d9; flex: 1; }
.header button { padding: 8px 16px; background: #238636; color: white; border: none; border-radius: 4px; cursor: pointer; }
.messages { height: 400px; overflow-y: auto; background: #161b22; padding: 10px; border-radius: 8px; margin-bottom: 10px; }
.msg { padding: 4px 0; border-bottom: 1px solid #21262d; }
.username { color: #58a6ff; font-weight: bold; }
.time { color: #8b949e; font-size: 12px; margin-left: 10px; }
.system { color: #f0883e; font-style: italic; }
.input-area { display: flex; gap: 10px; }
.input-area input { flex: 1; padding: 10px; border-radius: 4px; border: 1px solid #30363d; background: #21262d; color: #c9d1d9; }
.input-area button { padding: 10px 20px; background: #238636; color: white; border: none; border-radius: 4px; cursor: pointer; }
.status { margin-top: 10px; color: #8b949e; }
</style>
</head>
<body>
<div class="chat-container">
<h1>💬 Unified Chat (on Blockchain)</h1>
<div class="header">
<input type="text" id="usernameInput" placeholder="Username" value="Anonymous">
<input type="text" id="roomInput" placeholder="Room" value="general">
<button onclick="joinRoom()">Join Room</button>
</div>
<div id="messages" class="messages"></div>
<div class="input-area">
<input type="text" id="messageInput" placeholder="Type a message..." />
<button onclick="sendMessage()">Send</button>
</div>
<div id="status" class="status">🔴 Disconnected</div>
</div>
<script>
var ws = null, username='Anonymous', room='general';
function connect() {
username = document.getElementById('usernameInput').value.trim() || 'Anonymous';
room = document.getElementById('roomInput').value.trim() || 'general';
var url = 'ws://' + window.location.host + '/chat?username=' + encodeURIComponent(username) + '&room=' + encodeURIComponent(room);
ws = new WebSocket(url);
ws.onopen = function() { document.getElementById('status').textContent = '🟢 Connected to ' + room; };
ws.onmessage = function(e) {
var data = JSON.parse(e.data);
if (data.type === 'history') { data.data.forEach(m => addMessage(m)); }
else { addMessage(data); }
};
ws.onclose = function() { document.getElementById('status').textContent = '🔴 Disconnected, reconnecting...'; setTimeout(connect, 3000); };
}
function addMessage(msg) {
var div = document.createElement('div');
div.className = 'msg';
if (msg.text === 'joined the chat' || msg.text === 'left the chat') {
div.innerHTML = '<span class="system">' + msg.username + ' ' + msg.text + '</span>';
} else {
div.innerHTML = '<span class="username">' + msg.username + '</span><span class="time">' + (msg.time||'') + '</span><br>' + msg.text;
}
document.getElementById('messages').appendChild(div);
document.getElementById('messages').scrollTop = 1e9;
}
function sendMessage() {
if (!ws || ws.readyState !== WebSocket.OPEN) { alert('Not connected'); return; }
var input = document.getElementById('messageInput');
var text = input.value.trim(); if (!text) return;
ws.send(JSON.stringify({username: username, room: room, text: text}));
input.value = '';
}
function joinRoom() { if (ws) ws.close(); connect(); }
document.getElementById('messageInput').addEventListener('keydown', function(e) { if (e.key === 'Enter') sendMessage(); });
connect();
</script>
</body>
</html>`
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprint(w, html)
}

// ---------- CLI ----------
func runCLI() {
	if len(os.Args) < 2 {
		fmt.Println("Usage:")
		fmt.Println("  ./unified add <data>     - Add transaction")
		fmt.Println("  ./unified print          - Print chain")
		fmt.Println("  ./unified peers          - List peers")
		fmt.Println("  ./unified connect ws://... - Connect peer")
		fmt.Println("  ./unified wallet create  - Create wallet")
		fmt.Println("  ./unified wallet balance <addr>")
		fmt.Println("  ./unified wallet send <from> <to> <amount>")
		fmt.Println("  ./unified kv set <key> <value>")
		fmt.Println("  ./unified kv get <key>")
		return
	}
	port := os.Getenv("PORT")
	if port == "" { port = "8184" }
	baseURL := "http://localhost:" + port

	switch os.Args[1] {
	case "add":
		data := strings.Join(os.Args[2:], " ")
		resp, _ := http.Post(baseURL+"/blocks", "application/json", strings.NewReader(`{"Data":"`+data+`"}`))
		defer resp.Body.Close()
		var res map[string]string
		json.NewDecoder(resp.Body).Decode(&res)
		fmt.Println(res["status"])

	case "print":
		resp, _ := http.Get(baseURL + "/blocks")
		defer resp.Body.Close()
		var chain []Block
		json.NewDecoder(resp.Body).Decode(&chain)
		for _, b := range chain {
			fmt.Printf("Idx:%d Data:%s Hash:%s Prev:%s\n", b.Index, b.Data, b.Hash, b.PrevHash)
		}

	case "peers":
		resp, _ := http.Get(baseURL + "/peers")
		defer resp.Body.Close()
		var res map[string]int
		json.NewDecoder(resp.Body).Decode(&res)
		fmt.Printf("Peers: %d\n", res["count"])

	case "connect":
		if len(os.Args) < 3 { fmt.Println("Usage: connect ws://..."); return }
		url := os.Args[2]
		http.Post(baseURL+"/connect", "application/json", strings.NewReader(`{"address":"`+url+`"}`))

	case "wallet":
		if len(os.Args) < 3 { fmt.Println("wallet create|balance|send"); return }
		switch os.Args[2] {
		case "create":
			resp, _ := http.Post(baseURL+"/wallet/create", "application/json", nil)
			defer resp.Body.Close()
			var res map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&res)
			fmt.Printf("Address: %s Balance: %.0f\n", res["address"], res["balance"])
		case "balance":
			if len(os.Args) < 4 { fmt.Println("Usage: wallet balance <address>"); return }
			addr := os.Args[3]
			resp, _ := http.Get(baseURL + "/wallet/balance?address=" + addr)
			defer resp.Body.Close()
			var res map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&res)
			fmt.Printf("Balance: %.0f\n", res["balance"])
		case "send":
			if len(os.Args) < 6 { fmt.Println("Usage: wallet send <from> <to> <amount>"); return }
			from, to, amt := os.Args[3], os.Args[4], os.Args[5]
			resp, _ := http.Post(baseURL+"/wallet/send", "application/json",
				strings.NewReader(`{"from":"`+from+`","to":"`+to+`","amount":`+amt+`}`))
			defer resp.Body.Close()
			var res map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&res)
			fmt.Println(res["status"])
		}

	case "kv":
		if len(os.Args) < 3 { fmt.Println("kv set|get"); return }
		switch os.Args[2] {
		case "set":
			if len(os.Args) < 5 { fmt.Println("Usage: kv set <key> <value>"); return }
			key, val := os.Args[3], os.Args[4]
			resp, _ := http.Post(baseURL+"/kv/set", "application/json",
				strings.NewReader(`{"key":"`+key+`","value":"`+val+`"}`))
			defer resp.Body.Close()
			var res map[string]string
			json.NewDecoder(resp.Body).Decode(&res)
			fmt.Printf("Stored: %s = %s\n", res["key"], res["value"])
		case "get":
			if len(os.Args) < 4 { fmt.Println("Usage: kv get <key>"); return }
			key := os.Args[3]
			resp, _ := http.Get(baseURL + "/kv/get?key=" + key)
			defer resp.Body.Close()
			if resp.StatusCode == 404 { fmt.Println("Key not found"); return }
			var res map[string]string
			json.NewDecoder(resp.Body).Decode(&res)
			fmt.Printf("%s = %s\n", res["key"], res["value"])
		}

	default:
		fmt.Println("Unknown command")
	}
}

// ---------- Main ----------
func main() {
	if len(os.Args) > 1 {
		runCLI()
		return
	}

	// Load blockchain
	loadedChain, loadedMempool := loadState()
	bc = loadedChain
	mempool = loadedMempool

	// Start miner
	stopMiner = make(chan struct{})
	go mineBlocks()

	// Start chat broadcaster
	go broadcastChatMessages()

	// Print chain
	fmt.Println("\n==== UNIFIED BLOCKCHAIN ====")
	bc.PrintChain()
	fmt.Printf("Chain Valid: %v\n", bc.ValidateChain())

	// HTTP routes
	http.HandleFunc("/blocks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" { handleGetBlocks(w, r) } else if r.Method == "POST" { handlePostBlock(w, r) } else { http.Error(w, "", http.StatusMethodNotAllowed) }
	})
	http.HandleFunc("/blocks/{index}", handleGetBlockByIndex)
	http.HandleFunc("/mempool", handleGetMempool)
	http.HandleFunc("/status", handleStatus)
	http.HandleFunc("/peers", handleGetPeers)
	http.HandleFunc("/wallet/create", func(w http.ResponseWriter, r *http.Request) { if r.Method == "POST" { handleCreateWallet(w, r) } })
	http.HandleFunc("/wallet/balance", func(w http.ResponseWriter, r *http.Request) { if r.Method == "GET" { handleGetBalance(w, r) } })
	http.HandleFunc("/wallet/send", func(w http.ResponseWriter, r *http.Request) { if r.Method == "POST" { handleSendCoins(w, r) } })
	http.HandleFunc("/wallet/history", func(w http.ResponseWriter, r *http.Request) { if r.Method == "GET" { handleWalletHistory(w, r) } })
	http.HandleFunc("/kv/set", func(w http.ResponseWriter, r *http.Request) { if r.Method == "POST" { handleKVSet(w, r) } })
	http.HandleFunc("/kv/get", func(w http.ResponseWriter, r *http.Request) { if r.Method == "GET" { handleKVGet(w, r) } })
	http.HandleFunc("/chat", handleChatWS)
	http.HandleFunc("/chat/ui", handleChatUI)
	http.HandleFunc("/ws", handleWebSocket)
	http.HandleFunc("/", handleChatUI) // Default UI

	port := os.Getenv("PORT")
	if port == "" { port = "8184" }
	server := &http.Server{Addr: ":" + port}

	go func() {
		log.Printf("🚀 Unified server running on http://localhost:%s", port)
		log.Printf("📦 Features: Blockchain + KV Store + Chat")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Graceful shutdown
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt)
	<-stopChan
	log.Println("\n🛑 Shutting down...")
	close(stopMiner)
	time.Sleep(100 * time.Millisecond)
	saveState()
	log.Println("👋 Goodbye!")
}
