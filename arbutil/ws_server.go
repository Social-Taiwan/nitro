package arbutil

import (
	"context"
	r_log "log"
	"net/http"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/gorilla/websocket"
)

const (
	NEWHEADER_PATH = "/newheader"
	RECEIPT_PATH   = "/receipt"
	TXRESULT_PATH  = "/txresult"
)

type WebsocketServer struct {
	ctx               context.Context
	PathToConnMangers map[string]*connManager
}

type connManager struct {
	sync.RWMutex
	connIdx         int
	connStorages    map[int]*connStorage // connection num => connection
	connFailedCache map[int]int
	DataChan        chan interface{}
}

type connStorage struct {
	*websocket.Conn
	dataChan chan interface{}
}

type NewHeader struct {
	Logs []types.Log `json:"logs"`
}

type NewReceipt struct {
	TxHash          common.Hash    `json:"txHash"`
	ContractAddress common.Address `json:"contractAddress"`
	GasUsed         uint64         `json:"gasUsed"`
	Status          uint64         `json:"status"`
	Logs            []types.Log    `json:"logs"`
}

type NewReceipts struct {
	Receipts []NewReceipt `json:"receipts"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
	ReadBufferSize:  4096, // TODO: double check
	WriteBufferSize: 4096,
}

func NewWebSocketService() *WebsocketServer {
	receiptConnManager := &connManager{
		connIdx:         0,
		connStorages:    make(map[int]*connStorage),
		connFailedCache: make(map[int]int),
		DataChan:        make(chan interface{}, 1000),
	}

	txResultConnManager := &connManager{
		connIdx:         0,
		connStorages:    make(map[int]*connStorage),
		connFailedCache: make(map[int]int),
		DataChan:        make(chan interface{}, 1000),
	}

	newHeaderConnManager := &connManager{
		connIdx:         0,
		connStorages:    make(map[int]*connStorage),
		connFailedCache: make(map[int]int),
		DataChan:        make(chan interface{}, 1000),
	}

	s := &WebsocketServer{
		PathToConnMangers: map[string]*connManager{
			RECEIPT_PATH:   receiptConnManager,
			TXRESULT_PATH:  txResultConnManager,
			NEWHEADER_PATH: newHeaderConnManager,
		},
	}

	http.HandleFunc(RECEIPT_PATH, s.handleReceiptConnection)
	http.HandleFunc(TXRESULT_PATH, s.handleTxResultConnection)
	http.HandleFunc(NEWHEADER_PATH, s.handleNewHeaderConnection)

	go http.ListenAndServe(":8086", nil)

	return s
}

func (s *WebsocketServer) Start(ctx context.Context) {
	s.ctx = ctx
	for _, cm := range s.PathToConnMangers {
		go cm.BroadcastSession(ctx)
	}
}

func (s *WebsocketServer) Close() {
	for _, c := range s.PathToConnMangers {
		c.Lock()
		defer c.Unlock()

		for _, v := range c.connStorages {
			v.Close()
		}
		c.connStorages = nil
	}
}

func (s *WebsocketServer) handleReceiptConnection(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		r_log.Println(err)
		return
	}

	cs := &connStorage{
		Conn:     conn,
		dataChan: make(chan interface{}, 1000),
	}

	cm := s.PathToConnMangers[RECEIPT_PATH]
	idx := cm.AddFeedClient(cs)
	cm.connWriteSession(s.ctx, idx, cs)
}

func (s *WebsocketServer) handleTxResultConnection(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		r_log.Println(err)
		return
	}

	cs := &connStorage{
		Conn:     conn,
		dataChan: make(chan interface{}, 1000),
	}

	cm := s.PathToConnMangers[TXRESULT_PATH]
	idx := cm.AddFeedClient(cs)
	cm.connWriteSession(s.ctx, idx, cs)
}

func (s *WebsocketServer) handleNewHeaderConnection(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		r_log.Println(err)
		return
	}

	cs := &connStorage{
		Conn:     conn,
		dataChan: make(chan interface{}, 1000),
	}

	cm := s.PathToConnMangers[NEWHEADER_PATH]
	idx := cm.AddFeedClient(cs)
	cm.connWriteSession(s.ctx, idx, cs)
}

func (c *connManager) connWriteSession(ctx context.Context, idx int, cs *connStorage) {
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-cs.dataChan:
			if err := cs.WriteJSON(data); err != nil {
				r_log.Println("[Websocket] failed to send msg to feed: ", err)
				c.AddOneToFeedConnFailedCounts(idx)
			} else {
				c.UpdateFeedConnFailedCounts(idx, 0)
			}

			if c.ReadFeedConnFailedCounts(idx) > 3 {
				c.RemoveFeedClient(idx)
			}
		}
	}
}

func (c *connManager) BroadcastSession(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-c.DataChan:
			c.Broadcast(data)
		}
	}
}

func (c *connManager) Broadcast(v interface{}) {
	for idx, cs := range c.connStorages {
		go func(idx int, cs *connStorage) {
			cs.dataChan <- v
		}(idx, cs)
	}
}

func (c *connManager) AddFeedClient(cs *connStorage) int {
	c.Lock()
	defer c.Unlock()

	idx := c.connIdx
	c.connStorages[idx] = cs
	c.connFailedCache[idx] = 0

	c.connIdx++

	return idx
}

func (c *connManager) RemoveFeedClient(idx int) {
	c.Lock()
	defer c.Unlock()

	c.connStorages[idx].Close()
	delete(c.connStorages, idx)
	delete(c.connFailedCache, idx)
}

func (c *connManager) ReadFeedConnFailedCounts(idx int) int {
	c.RLock()
	defer c.RUnlock()

	return c.connFailedCache[idx]
}

func (c *connManager) UpdateFeedConnFailedCounts(idx, count int) {
	c.Lock()
	defer c.Unlock()

	c.connFailedCache[idx] = count
}

func (c *connManager) AddOneToFeedConnFailedCounts(idx int) {
	c.Lock()
	defer c.Unlock()

	c.connFailedCache[idx]++
}
