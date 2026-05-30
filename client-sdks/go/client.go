// Package cqclient is a Go client for CQServer, a pub/sub database, speaking
// its length-prefixed JSON frame protocol over plain TCP.
package cqclient

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"time"
)

// Top bit of the u32 length prefix flags a zstd-compressed body. This SDK
// advertises no compression, so the server never sets it; an inbound frame
// with the flag set is therefore a protocol error.
const (
	compressedFlag uint32 = 0x80000000
	maxFrame              = 16 * 1024 * 1024
)

// SupportedVersions are the wire protocol versions this SDK understands.
var SupportedVersions = []int{1}

// CqError is returned for server-reported errors and protocol violations.
type CqError struct {
	msg string
}

func (e *CqError) Error() string { return e.msg }

func newErr(format string, args ...interface{}) *CqError {
	return &CqError{msg: fmt.Sprintf(format, args...)}
}

// Delta is a single subscription update (snapshot row or live change).
type Delta struct {
	DeltaType string                 // "sow" (snapshot), "add", "update", "remove", ...
	SubID     string                 //
	Data      map[string]interface{} //
	Sequence  int64                  //
}

// wireMsg is the on-the-wire envelope. Pointer fields with omitempty keep the
// encoded form minimal and let us distinguish "absent" from "zero".
type wireMsg struct {
	Cmd       *string                `json:"c,omitempty"`
	Cid       *string                `json:"cid,omitempty"`
	Topic     *string                `json:"t,omitempty"`
	Sid       *string                `json:"sid,omitempty"`
	Filter    *string                `json:"f,omitempty"`
	SQL       *string                `json:"sql,omitempty"`
	Data      json.RawMessage        `json:"d,omitempty"`
	Status    *string                `json:"s,omitempty"`
	Reason    *string                `json:"r,omitempty"`
	Seq       *int64                 `json:"seq,omitempty"`
	Bookmark  *int64                 `json:"bm,omitempty"`
	DeltaType *string                `json:"dt,omitempty"`
	Versions  []int                  `json:"pv,omitempty"`
	Client    *string                `json:"client,omitempty"`
}

func strp(s string) *string { return &s }
func i64p(i int64) *int64    { return &i }

// pending is an RPC waiter (publish, logon, sow setup).
type pending struct {
	done     chan struct{}
	response *wireMsg
	once     sync.Once
}

func newPending() *pending { return &pending{done: make(chan struct{})} }

func (p *pending) fire(resp *wireMsg) {
	p.once.Do(func() {
		p.response = resp
		close(p.done)
	})
}

// snapDone signals SOW completion; errMsg carries an optional error string.
type snapDone struct {
	done   chan struct{}
	errMsg string
	once   sync.Once
}

func newSnapDone() *snapDone { return &snapDone{done: make(chan struct{})} }

func (s *snapDone) fire(errMsg string) {
	s.once.Do(func() {
		s.errMsg = errMsg
		close(s.done)
	})
}

// Subscription delivers snapshot rows then live deltas via a buffered channel.
type Subscription struct {
	SubID string
	Cid   string
	ch    chan *Delta
}

func newSubscription(cid string) *Subscription {
	return &Subscription{Cid: cid, ch: make(chan *Delta, 1024)}
}

func (s *Subscription) push(d *Delta) {
	// Non-blocking-ish: the channel is generously buffered. Block if full to
	// preserve ordering rather than drop deltas.
	s.ch <- d
}

// NextDelta blocks for the next delta up to timeout. Returns (nil, nil) on
// timeout. A zero or negative timeout blocks indefinitely.
func (s *Subscription) NextDelta(timeout time.Duration) (*Delta, error) {
	if timeout <= 0 {
		d, ok := <-s.ch
		if !ok {
			return nil, newErr("subscription closed")
		}
		return d, nil
	}
	select {
	case d, ok := <-s.ch:
		if !ok {
			return nil, newErr("subscription closed")
		}
		return d, nil
	case <-time.After(timeout):
		return nil, nil
	}
}

// Client is a CQServer connection. It runs one background reader goroutine.
type Client struct {
	conn net.Conn

	sendMu sync.Mutex

	mu              sync.Mutex
	cidSeq          int64
	protocolVersion int
	closed          bool

	pending        map[string]*pending      // cid -> RPC waiter
	pendingSubs    map[string]*Subscription // cid -> sub (pre-sid)
	subs           map[string]*Subscription // sid -> sub (live)
	snapshotBuffer map[string][]map[string]interface{}
	snapshotDone   map[string]*snapDone
}

// Connect dials host:port over TCP and starts the reader goroutine.
func Connect(host string, port int, timeout time.Duration) (*Client, error) {
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	c := &Client{
		conn:            conn,
		protocolVersion: 1,
		pending:         make(map[string]*pending),
		pendingSubs:     make(map[string]*Subscription),
		subs:            make(map[string]*Subscription),
		snapshotBuffer:  make(map[string][]map[string]interface{}),
		snapshotDone:    make(map[string]*snapDone),
	}
	go c.readLoop()
	return c, nil
}

// ProtocolVersion returns the negotiated protocol version.
func (c *Client) ProtocolVersion() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.protocolVersion
}

// Close shuts down the connection and wakes any blocked waiters.
func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return c.conn.Close()
}

// ---- framing ----------------------------------------------------

func (c *Client) send(msg *wireMsg) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if len(body) > maxFrame {
		return newErr("frame too large: %d bytes", len(body))
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(body)))
	frame := append(header, body...)
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	_, err = c.conn.Write(frame)
	return err
}

func (c *Client) readLoop() {
	defer c.failAllWaiters()
	header := make([]byte, 4)
	for {
		if _, err := io.ReadFull(c.conn, header); err != nil {
			return
		}
		raw := binary.BigEndian.Uint32(header)
		if raw&compressedFlag != 0 {
			// Protocol violation: we never negotiated a codec.
			return
		}
		length := raw &^ compressedFlag
		if length > maxFrame {
			return
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(c.conn, payload); err != nil {
			return
		}
		var msg wireMsg
		if err := json.Unmarshal(payload, &msg); err != nil {
			return
		}
		c.dispatch(&msg)
	}
}

func (c *Client) failAllWaiters() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.pending {
		p.fire(nil)
	}
	for _, s := range c.snapshotDone {
		s.fire("connection closed")
	}
}

// ---- dispatch ---------------------------------------------------

func (c *Client) dispatch(msg *wireMsg) {
	if msg.Cmd == nil {
		return
	}
	switch *msg.Cmd {
	case "ack":
		c.onAck(msg)
	case "sow":
		c.onSowRow(msg)
	case "sow_batch":
		c.onSowBatch(msg)
	case "group_end":
		c.onGroupEnd(msg)
	case "publish":
		c.onDelta(msg)
	}
	// group_begin / heartbeat / schema_change: nothing to do here.
}

func (c *Client) onAck(msg *wireMsg) {
	// Promote a pre-registered subscription into the live routing map BEFORE
	// handling any later frames in this batch so same-batch snapshot rows route.
	if msg.Cid != nil && msg.Sid != nil {
		c.mu.Lock()
		if sub, ok := c.pendingSubs[*msg.Cid]; ok {
			delete(c.pendingSubs, *msg.Cid)
			sub.SubID = *msg.Sid
			c.subs[*msg.Sid] = sub
		}
		c.mu.Unlock()
	}
	if msg.Cid == nil {
		return
	}
	cid := *msg.Cid
	// Error ack for an in-flight SOW: no group_end will arrive.
	if msg.Status != nil && *msg.Status == "error" {
		c.mu.Lock()
		s := c.snapshotDone[cid]
		if s != nil {
			delete(c.snapshotDone, cid)
		}
		c.mu.Unlock()
		if s != nil {
			reason := "server error"
			if msg.Reason != nil && *msg.Reason != "" {
				reason = *msg.Reason
			}
			s.fire(reason)
		}
	}
	c.mu.Lock()
	p := c.pending[cid]
	if p != nil {
		delete(c.pending, cid)
	}
	c.mu.Unlock()
	if p != nil {
		p.fire(msg)
	}
}

func (c *Client) onSowRow(msg *wireMsg) {
	if msg.Sid == nil || len(msg.Data) == 0 {
		return
	}
	row, ok := decodeObject(msg.Data)
	if !ok {
		return
	}
	sid := *msg.Sid
	c.mu.Lock()
	buf, hasBuf := c.snapshotBuffer[sid]
	sub := c.subs[sid]
	if hasBuf {
		c.snapshotBuffer[sid] = append(buf, row)
	}
	c.mu.Unlock()
	if sub != nil {
		var seq int64
		if msg.Seq != nil {
			seq = *msg.Seq
		}
		sub.push(&Delta{DeltaType: "sow", SubID: sid, Data: row, Sequence: seq})
	}
}

func (c *Client) onSowBatch(msg *wireMsg) {
	if msg.Sid == nil || len(msg.Data) == 0 {
		return
	}
	rows, ok := decodeArray(msg.Data)
	if !ok {
		return
	}
	sid := *msg.Sid
	for _, row := range rows {
		c.mu.Lock()
		buf, hasBuf := c.snapshotBuffer[sid]
		sub := c.subs[sid]
		if hasBuf {
			c.snapshotBuffer[sid] = append(buf, row)
		}
		c.mu.Unlock()
		if sub != nil {
			sub.push(&Delta{DeltaType: "sow", SubID: sid, Data: row})
		}
	}
}

func (c *Client) onGroupEnd(msg *wireMsg) {
	if msg.Sid == nil {
		return
	}
	c.mu.Lock()
	s := c.snapshotDone[*msg.Sid]
	if s != nil {
		delete(c.snapshotDone, *msg.Sid)
	}
	c.mu.Unlock()
	if s != nil {
		s.fire("") // success
	}
}

func (c *Client) onDelta(msg *wireMsg) {
	if msg.Sid == nil {
		return
	}
	sid := *msg.Sid
	c.mu.Lock()
	sub := c.subs[sid]
	c.mu.Unlock()
	if sub == nil {
		return
	}
	data := map[string]interface{}{}
	if len(msg.Data) > 0 {
		if obj, ok := decodeObject(msg.Data); ok {
			data = obj
		}
	}
	dt := "update"
	if msg.DeltaType != nil && *msg.DeltaType != "" {
		dt = *msg.DeltaType
	}
	var seq int64
	if msg.Seq != nil {
		seq = *msg.Seq
	}
	sub.push(&Delta{DeltaType: dt, SubID: sid, Data: data, Sequence: seq})
}

// ---- helpers ----------------------------------------------------

func decodeObject(raw json.RawMessage) (map[string]interface{}, bool) {
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return nil, false
	}
	return obj, true
}

func decodeArray(raw json.RawMessage) ([]map[string]interface{}, bool) {
	var arr []map[string]interface{}
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, false
	}
	return arr, true
}

func marshalData(data map[string]interface{}) (json.RawMessage, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}

func (c *Client) nextCid() string {
	c.mu.Lock()
	c.cidSeq++
	v := c.cidSeq
	c.mu.Unlock()
	return fmt.Sprintf("%d", v)
}

// rpc sends msg (assigning a cid if absent) and waits for the matching ack.
func (c *Client) rpc(msg *wireMsg, timeout time.Duration) (*wireMsg, error) {
	if msg.Cid == nil {
		msg.Cid = strp(c.nextCid())
	}
	cid := *msg.Cid
	p := newPending()
	c.mu.Lock()
	c.pending[cid] = p
	c.mu.Unlock()
	if err := c.send(msg); err != nil {
		c.mu.Lock()
		delete(c.pending, cid)
		c.mu.Unlock()
		return nil, err
	}
	select {
	case <-p.done:
	case <-time.After(timeout):
		c.mu.Lock()
		delete(c.pending, cid)
		c.mu.Unlock()
		return nil, newErr("timed out waiting for ack")
	}
	resp := p.response
	if resp == nil {
		return nil, newErr("connection closed")
	}
	if resp.Status != nil && *resp.Status == "error" {
		reason := "server error"
		if resp.Reason != nil && *resp.Reason != "" {
			reason = *resp.Reason
		}
		return nil, newErr("%s", reason)
	}
	return resp, nil
}

// ---- API --------------------------------------------------------

// Logon authenticates and negotiates the protocol version. With empty
// credentials this is an anonymous handshake. Returns the negotiated version.
func (c *Client) Logon(user, password, token, clientName string) (int, error) {
	msg := &wireMsg{Cmd: strp("logon"), Versions: SupportedVersions}
	creds := map[string]interface{}{}
	if token != "" {
		creds["token"] = token
	} else if user != "" {
		creds["user"] = user
		creds["password"] = password
	}
	if len(creds) > 0 {
		raw, err := marshalData(creds)
		if err != nil {
			return 0, err
		}
		msg.Data = raw
	}
	if clientName != "" {
		msg.Client = strp(clientName)
	}
	resp, err := c.rpc(msg, 5*time.Second)
	if err != nil {
		return 0, err
	}
	if len(resp.Versions) > 0 {
		c.mu.Lock()
		c.protocolVersion = resp.Versions[0]
		c.mu.Unlock()
	}
	return c.ProtocolVersion(), nil
}

// Publish upserts a record and returns the assigned sequence.
func (c *Client) Publish(topic string, data map[string]interface{}) (int64, error) {
	return c.publishCmd("publish", topic, data, 5*time.Second)
}

// DeltaPublish sparsely updates a row (key + changed fields).
func (c *Client) DeltaPublish(topic string, data map[string]interface{}) (int64, error) {
	return c.publishCmd("delta_publish", topic, data, 5*time.Second)
}

func (c *Client) publishCmd(cmd, topic string, data map[string]interface{}, timeout time.Duration) (int64, error) {
	raw, err := marshalData(data)
	if err != nil {
		return 0, err
	}
	resp, err := c.rpc(&wireMsg{Cmd: strp(cmd), Topic: strp(topic), Data: raw}, timeout)
	if err != nil {
		return 0, err
	}
	if resp.Seq != nil {
		return *resp.Seq, nil
	}
	return 0, nil
}

// Sow runs a one-shot SOW query and returns the snapshot rows. Pass an empty
// filter for no filter.
func (c *Client) Sow(topic, filter string) ([]map[string]interface{}, error) {
	msg := &wireMsg{Cmd: strp("sow"), Topic: strp(topic)}
	if filter != "" {
		msg.Filter = strp(filter)
	}
	return c.runSow(msg, 10*time.Second)
}

// SowSQL runs an arbitrary SOW SQL query (GROUP BY / aggregates).
func (c *Client) SowSQL(topic, sql string) ([]map[string]interface{}, error) {
	msg := &wireMsg{Cmd: strp("sow"), Topic: strp(topic), SQL: strp(sql)}
	return c.runSow(msg, 10*time.Second)
}

func (c *Client) runSow(msg *wireMsg, timeout time.Duration) ([]map[string]interface{}, error) {
	cid := c.nextCid()
	msg.Cid = strp(cid)
	done := newSnapDone()
	c.mu.Lock()
	c.snapshotBuffer[cid] = []map[string]interface{}{}
	c.snapshotDone[cid] = done
	c.mu.Unlock()
	if err := c.send(msg); err != nil {
		c.mu.Lock()
		delete(c.snapshotBuffer, cid)
		delete(c.snapshotDone, cid)
		c.mu.Unlock()
		return nil, err
	}
	select {
	case <-done.done:
	case <-time.After(timeout):
		c.mu.Lock()
		delete(c.snapshotBuffer, cid)
		delete(c.snapshotDone, cid)
		c.mu.Unlock()
		return nil, newErr("timed out waiting for SOW group_end")
	}
	c.mu.Lock()
	rows := c.snapshotBuffer[cid]
	delete(c.snapshotBuffer, cid)
	c.mu.Unlock()
	if done.errMsg != "" {
		return nil, newErr("%s", done.errMsg)
	}
	return rows, nil
}

// SowAndSubscribe opens a snapshot + continuous delta subscription. Snapshot
// rows arrive as deltas with DeltaType == "sow"; live changes as
// "add"/"update"/"remove". Pass empty filter / bookmark <= 0 to omit them.
func (c *Client) SowAndSubscribe(topic, filter string, bookmark int64) (*Subscription, error) {
	cid := c.nextCid()
	sub := newSubscription(cid)
	c.mu.Lock()
	c.pendingSubs[cid] = sub
	c.mu.Unlock()
	msg := &wireMsg{Cmd: strp("sow_and_subscribe"), Cid: strp(cid), Topic: strp(topic)}
	if filter != "" {
		msg.Filter = strp(filter)
	}
	if bookmark > 0 {
		msg.Bookmark = i64p(bookmark)
	}
	if err := c.send(msg); err != nil {
		c.mu.Lock()
		delete(c.pendingSubs, cid)
		c.mu.Unlock()
		return nil, err
	}
	return sub, nil
}

// Subscribe opens a live-deltas-only subscription (no initial snapshot).
func (c *Client) Subscribe(topic, filter string) (*Subscription, error) {
	cid := c.nextCid()
	sub := newSubscription(cid)
	c.mu.Lock()
	c.pendingSubs[cid] = sub
	c.mu.Unlock()
	msg := &wireMsg{Cmd: strp("subscribe"), Cid: strp(cid), Topic: strp(topic)}
	if filter != "" {
		msg.Filter = strp(filter)
	}
	if err := c.send(msg); err != nil {
		c.mu.Lock()
		delete(c.pendingSubs, cid)
		c.mu.Unlock()
		return nil, err
	}
	return sub, nil
}

// Unsubscribe cancels a live subscription.
func (c *Client) Unsubscribe(sub *Subscription) error {
	if sub == nil || sub.SubID == "" {
		return nil
	}
	c.mu.Lock()
	delete(c.subs, sub.SubID)
	c.mu.Unlock()
	return c.send(&wireMsg{Cmd: strp("unsubscribe"), Sid: strp(sub.SubID)})
}

// SortedStringValues returns the sorted string values of key across rows,
// skipping rows where the key is absent or non-string. A small convenience
// used by the example.
func SortedStringValues(rows []map[string]interface{}, key string) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if v, ok := r[key]; ok {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}
	}
	sort.Strings(out)
	return out
}
