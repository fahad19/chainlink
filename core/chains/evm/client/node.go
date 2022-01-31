package client

import (
	"context"
	"fmt"
	"math/big"
	"net/url"
	"sync"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/atomic"

	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/utils"
)

//go:generate mockery --name Node --output ../mocks/ --case=underscore

// Node represents a client that connects to an ethereum-compatible RPC node
type Node interface {
	Start(ctx context.Context) error
	Close()

	// TODO: Expose state on API/GUI
	State() NodeState

	CallContext(ctx context.Context, result interface{}, method string, args ...interface{}) error
	BatchCallContext(ctx context.Context, b []rpc.BatchElem) error
	SendTransaction(ctx context.Context, tx *types.Transaction) error
	PendingCodeAt(ctx context.Context, account common.Address) ([]byte, error)
	PendingNonceAt(ctx context.Context, account common.Address) (uint64, error)
	NonceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (uint64, error)
	TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error)
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)
	BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error)
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
	SubscribeFilterLogs(ctx context.Context, q ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error)
	EstimateGas(ctx context.Context, call ethereum.CallMsg) (uint64, error)
	SuggestGasPrice(ctx context.Context) (*big.Int, error)
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
	CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
	SuggestGasTipCap(ctx context.Context) (*big.Int, error)
	EthSubscribe(ctx context.Context, channel interface{}, args ...interface{}) (ethereum.Subscription, error)
	ChainID(ctx context.Context) (chainID *big.Int, err error)

	String() string
}

type rawclient struct {
	rpc  *rpc.Client
	geth *ethclient.Client
	uri  url.URL
}

// NodeState represents the current state of the node
// Node is a FSM
type NodeState int

func (n NodeState) String() string {
	switch n {
	case NodeStateUndialed:
		return "Undialed"
	case NodeStateDialed:
		return "Dialed"
	case NodeStateInvalidChainID:
		return "InvalidChainID"
	case NodeStateAlive:
		return "Alive"
	case NodeStateUnreachable:
		return "Unreachable"
	case NodeStateOutOfSync:
		return "OutOfSync"
	case NodeStateClosed:
		return "Closed"
	default:
		return fmt.Sprintf("NodeState(%d)", n)
	}
}

// GoString prints a prettier state
func (n NodeState) GoString() string {
	return fmt.Sprintf("NodeState%s(%d)", n.String(), n)
}

const (
	// NodeStateUndialed is the first state of a virgin node
	NodeStateUndialed = NodeState(iota)
	// NodeStateDialed is after a node has successfully dialed but before it has verified the correct chain ID
	NodeStateDialed
	// NodeStateInvalidChainID is after chain ID verification failed
	NodeStateInvalidChainID
	// NodeStateAlive is a healthy node after chain ID verification succeeded
	NodeStateAlive
	// NodeStateUnreachable is a node that cannot be dialed or has disconnected
	NodeStateUnreachable
	// NodeStateOutOfSync is a node that is accepting connections but exceeded
	// the failure threshold without sending any new heads. It will be
	// disconnected, then put into a revive loop and re-awakened after redial
	// if a new head arrives
	NodeStateOutOfSync
	// NodeStateClosed is after the connection has been closed and the node is at the end of its lifecycle
	NodeStateClosed
)

// Node represents one ethereum node.
// It must have a ws url and may have a http url
type node struct {
	utils.StartStopOnce
	ws      rawclient
	http    *rawclient
	log     logger.Logger
	name    string
	chainID *big.Int
	cfg     NodeConfig
	startMu sync.Mutex

	state   NodeState
	stateMu sync.RWMutex

	// ctx can be cancelled to immediately cancel all in-flight requests on
	// this node. ctx should only be accessed through stateMu since the context
	// can be cancelled and replaced on state transitions.
	ctx    context.Context
	cancel context.CancelFunc
	// wg waits for subsidiary goroutines
	wg sync.WaitGroup

	// only set when liveness checking is enabled
	lastSeenBlockNumber atomic.Uint64
}

// NodeConfig allows configuration of the node
type NodeConfig interface {
	NodeNoNewHeadsThreshold() time.Duration
	NodePollFailureThreshold() uint32
	NodePollInterval() time.Duration
}

// NewNode returns a new *node as Node
func NewNode(nodeCfg NodeConfig, lggr logger.Logger, wsuri url.URL, httpuri *url.URL, name string, chainID *big.Int) Node {
	n := new(node)
	n.name = name
	n.chainID = chainID
	n.cfg = nodeCfg
	n.ws.uri = wsuri
	if httpuri != nil {
		n.http = &rawclient{uri: *httpuri}
	}
	n.ctx, n.cancel = context.WithCancel(context.Background())
	n.log = lggr.Named("Node").With(
		"nodeTier", "primary",
		"nodeName", name,
		"node", n.String(),
		"evmChainID", chainID,
	)
	return n
}

// Start dials and verifies the node
// Should only be called once in a node's lifecycle
// Return value is necessary to conform to interface but this will never
// actually return an error.
func (n *node) Start(startCtx context.Context) error {
	return n.StartOnce(n.name, func() error {
		n.start(startCtx)
		return nil
	})
}

// start initially dials the node and verifies chain ID
// This spins off lifecycle goroutines.
// Not thread-safe.
// Node lifecycle is synchronous: only one goroutine should be running at a
// time.
func (n *node) start(startCtx context.Context) {
	if n.state != NodeStateUndialed {
		panic(fmt.Sprintf("cannot dial node with state %v", n.state))
	}

	dialCtx, cancel := n.wrapCtx(startCtx)
	defer cancel()
	if err := n.dial(dialCtx); err != nil {
		n.log.Errorw("Dial failed: EVM Node is unreachable", "err", err)
		n.declareUnreachable()
		return
	}
	n.setState(NodeStateDialed)

	verifyCtx, cancel := n.wrapCtx(startCtx)
	defer cancel()
	if err := n.verify(verifyCtx); errors.Is(err, errInvalidChainID) {
		n.log.Errorw("Verify failed: EVM Node has the wrong chain ID", "err", err)
		n.declareInvalidChainID()
		return
	} else if err != nil {
		n.log.Errorw(fmt.Sprintf("Verify failed: %v", err), "err", err)
		n.declareUnreachable()
		return
	}

	n.declareAlive()
}

// Not thread-safe
// Pure dial: does not mutate node "state" field.
func (n *node) dial(ctx context.Context) error {
	var httpuri string
	if n.http != nil {
		httpuri = n.http.uri.String()
	}
	n.log.Debugw("RPC dial: evmclient.Client#dial", "wsuri", n.ws.uri.String(), "httpuri", httpuri)

	uri := n.ws.uri.String()
	wsrpc, err := rpc.DialWebsocket(ctx, uri, "")
	if err != nil {
		return errors.Wrapf(err, "error while dialing websocket: %v", uri)
	}

	var httprpc *rpc.Client
	if n.http != nil {
		httprpc, err = rpc.DialHTTP(httpuri)
		if err != nil {
			return errors.Wrapf(err, "error while dialing HTTP: %v", uri)
		}
	}

	n.ws.rpc = wsrpc
	n.ws.geth = ethclient.NewClient(wsrpc)

	if n.http != nil {
		n.http.rpc = httprpc
		n.http.geth = ethclient.NewClient(httprpc)
	}

	n.log.Debugw("RPC dial: success", "wsuri", n.ws.uri.String(), "httpuri", httpuri)

	return nil
}

var errInvalidChainID = errors.New("invalid chain id")

// verify checks that all connections to eth nodes match the given chain ID
// Not thread-safe
// Pure verify: does not mutate node "state" field.
func (n *node) verify(ctx context.Context) (err error) {
	ctx, cancel := n.wrapCtx(ctx)
	defer cancel()

	if !(n.state == NodeStateDialed) {
		panic(fmt.Sprintf("cannot verify node in state %v", n.state))
	}

	var chainID *big.Int
	if chainID, err = n.ws.geth.ChainID(ctx); err != nil {
		return errors.Wrapf(err, "failed to verify chain ID for node %s", n.name)
	} else if chainID.Cmp(n.chainID) != 0 {
		return errors.Wrapf(
			errInvalidChainID,
			"websocket rpc ChainID doesn't match local chain ID: RPC ID=%s, local ID=%s, node name=%s",
			chainID.String(),
			n.chainID.String(),
			n.name,
		)
	}
	if n.http != nil {
		if chainID, err = n.http.geth.ChainID(ctx); err != nil {
			return errors.Wrapf(err, "failed to verify chain ID for node %s", n.name)
		} else if chainID.Cmp(n.chainID) != 0 {
			return errors.Wrapf(
				errInvalidChainID,
				"http rpc ChainID doesn't match local chain ID: RPC ID=%s, local ID=%s, node name=%s",
				chainID.String(),
				n.chainID.String(),
				n.name,
			)
		}
	}

	return nil
}

func (n *node) Close() {
	//nolint:errcheck
	n.StopOnce(n.name, func() error {
		n.stateMu.Lock()
		n.cancel()
		n.stateMu.Unlock()
		n.wg.Wait()
		n.stateMu.Lock()
		defer n.stateMu.Unlock()
		n.state = NodeStateClosed
		if n.ws.rpc != nil {
			n.ws.rpc.Close()
		}
		return nil
	})
}

// FSM methods

// State allows reading the current state of the node
func (n *node) State() NodeState {
	n.stateMu.RLock()
	defer n.stateMu.RUnlock()
	return n.state
}

// setState is only used by internal state management methods.
// This is low-level; care should be taken by the caller to ensure the new state is a valid transition.
// State changes should always be synchronous: only one goroutine at a time should change state.
// n.stateMu should not be locked for long periods of time because external clients expect a timely response from n.State()
func (n *node) setState(s NodeState) {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()
	n.state = s
}

// declareXXX methods change the state and spin off the new state management
// goroutine

func (n *node) declareAlive() {
	n.transitionToAlive()
	n.log.Infow("EVM Node is online", "nodeState", n.State())
	n.wg.Add(1)
	go n.aliveLoop()
}

func (n *node) transitionToAlive() {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()
	if n.state == NodeStateDialed {
		n.state = NodeStateAlive
	} else {
		panic(fmt.Sprintf("cannot transition from %#v to NodeStateAlive", n.state))
	}

}

// declareInSync puts a node back into Alive state, allowing it to be used by
// pool consumers again
func (n *node) declareInSync() {
	n.transitionToInSync()
	n.log.Infow("EVM Node is back in sync", "nodeState", n.State())
	n.wg.Add(1)
	go n.aliveLoop()
}

func (n *node) transitionToInSync() {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()
	if n.state == NodeStateOutOfSync {
		n.state = NodeStateAlive
	} else {
		panic(fmt.Sprintf("cannot transition from %#v to NodeStateAlive", n.state))
	}
}

// declareOutOfSync puts a node into OutOfSync state, disconnecting all current
// clients and making it unavailable for use
func (n *node) declareOutOfSync(latestReceivedBlockNumber int64) {
	n.transitionToOutOfSync()
	n.log.Errorw("EVM Node is out of sync", "nodeState", n.State())
	n.wg.Add(1)
	go n.outOfSyncLoop(latestReceivedBlockNumber)
}

func (n *node) transitionToOutOfSync() {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()
	if n.state == NodeStateAlive {
		// Need to disconnect all clients subscribed to this node
		n.ws.rpc.Close()
		n.cancel() // cancel all pending calls that didn't get killed by closing RPC above
		// Replace the context
		// NOTE: This is why all ctx access must happen inside the mutex
		n.ctx, n.cancel = context.WithCancel(context.Background())
		n.state = NodeStateOutOfSync
	} else {
		panic(fmt.Sprintf("cannot transition from %#v to NodeStateOutOfSync", n.state))
	}
}

func (n *node) declareUnreachable() {
	n.transitionToUnreachable()
	n.log.Errorw("EVM Node is unreachable", "nodeState", n.State())
	n.wg.Add(1)
	go n.unreachableLoop()
}

func (n *node) transitionToUnreachable() {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()
	if n.state == NodeStateDialed || n.state == NodeStateAlive || n.state == NodeStateOutOfSync {
		// Need to disconnect all clients subscribed to this node
		n.ws.rpc.Close()
		n.cancel() // cancel all pending calls that didn't get killed by closing RPC above
		// Replace the context
		// NOTE: This is why all ctx access must happen inside the mutex
		n.ctx, n.cancel = context.WithCancel(context.Background())
		n.state = NodeStateUnreachable
	} else {
		panic(fmt.Sprintf("cannot transition from %#v to NodeStateUnreachable", n.state))
	}
}

func (n *node) declareInvalidChainID() {
	n.transitionToInvalidChainID()
	n.log.Errorw("EVM Node has the wrong chain ID", "nodeState", n.State())
	n.wg.Add(1)
	go n.invalidChainIDLoop()
}

func (n *node) transitionToInvalidChainID() {
	n.stateMu.Lock()
	defer n.stateMu.Unlock()
	if n.state == NodeStateDialed {
		// Need to disconnect all clients subscribed to this node
		n.ws.rpc.Close()
		n.cancel() // cancel all pending calls that didn't get killed by closing RPC above
		// Replace the context
		// NOTE: This is why all ctx access must happen inside the mutex
		n.ctx, n.cancel = context.WithCancel(context.Background())
		n.state = NodeStateInvalidChainID
	} else {
		panic(fmt.Sprintf("cannot transition from %#v to NodeStateInvalidChainID", n.state))
	}
}

// RPC wrappers

// CallContext implementation
func (n *node) CallContext(ctx context.Context, result interface{}, method string, args ...interface{}) error {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n)).With(
		"method", method,
		"args", args,
	)

	lggr.Debug("RPC call: evmclient.Client#CallContext")
	if n.http != nil {
		err = n.wrapHTTP(n.http.rpc.CallContext(ctx, result, method, args...))
	}
	err = n.wrapWS(n.ws.rpc.CallContext(ctx, result, method, args...))

	logResult(lggr, err)

	return err
}

func (n *node) BatchCallContext(ctx context.Context, b []rpc.BatchElem) error {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n)).With("nBatchElems", len(b))

	lggr.Debug("RPC call: evmclient.Client#BatchCallContext")
	if n.http != nil {
		err = n.wrapHTTP(n.http.rpc.BatchCallContext(ctx, b))
	}
	err = n.wrapWS(n.ws.rpc.BatchCallContext(ctx, b))

	logResult(lggr, err)

	return err
}

func (n *node) EthSubscribe(ctx context.Context, channel interface{}, args ...interface{}) (ethereum.Subscription, error) {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	lggr := n.newRqLggr("websocket").With("args", args)

	lggr.Debug("RPC call: evmclient.Client#EthSubscribe")
	sub, err := n.ws.rpc.EthSubscribe(ctx, channel, args...)

	logResult(lggr, err)

	return sub, err
}

// GethClient wrappers

func (n *node) TransactionReceipt(ctx context.Context, txHash common.Hash) (receipt *types.Receipt, err error) {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n)).With("txHash", txHash)

	lggr.Debug("RPC call: evmclient.Client#TransactionReceipt")

	if n.http != nil {
		receipt, err = n.http.geth.TransactionReceipt(ctx, txHash)
		err = n.wrapHTTP(err)
	} else {
		receipt, err = n.ws.geth.TransactionReceipt(ctx, txHash)
		err = n.wrapWS(err)
	}

	logResult(lggr, err, "receipt", receipt)

	return
}

func (n *node) HeaderByNumber(ctx context.Context, number *big.Int) (header *types.Header, err error) {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n)).With("number", number)

	lggr.Debug("RPC call: evmclient.Client#HeaderByNumber")
	if n.http != nil {
		header, err = n.http.geth.HeaderByNumber(ctx, number)
		err = n.wrapHTTP(err)
	} else {
		header, err = n.ws.geth.HeaderByNumber(ctx, number)
		err = n.wrapWS(err)
	}

	logResult(lggr, err, "header", header)

	return
}

func (n *node) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n)).With("tx", tx)

	lggr.Debug("RPC call: evmclient.Client#SendTransaction")
	if n.http != nil {
		err = n.wrapHTTP(n.http.geth.SendTransaction(ctx, tx))
	}
	err = n.wrapWS(n.ws.geth.SendTransaction(ctx, tx))

	logResult(lggr, err)

	return err
}

func (n *node) PendingNonceAt(ctx context.Context, account common.Address) (nonce uint64, err error) {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return 0, err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n)).With("account", account)

	lggr.Debug("RPC call: evmclient.Client#PendingNonceAt")
	if n.http != nil {
		nonce, err = n.http.geth.PendingNonceAt(ctx, account)
		err = n.wrapHTTP(err)
	} else {
		nonce, err = n.ws.geth.PendingNonceAt(ctx, account)
		err = n.wrapWS(err)
	}

	logResult(lggr, err, "nonce", nonce)

	return
}

func (n *node) NonceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (nonce uint64, err error) {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return 0, err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n)).With("account", account, "blockNumber", blockNumber)

	lggr.Debug("RPC call: evmclient.Client#NonceAt")
	if n.http != nil {
		nonce, err = n.http.geth.NonceAt(ctx, account, blockNumber)
		err = n.wrapHTTP(err)
	} else {
		nonce, err = n.ws.geth.NonceAt(ctx, account, blockNumber)
		err = n.wrapWS(err)
	}

	logResult(lggr, err, "nonce", nonce)

	return
}

func (n *node) PendingCodeAt(ctx context.Context, account common.Address) (code []byte, err error) {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n)).With("account", account)

	lggr.Debug("RPC call: evmclient.Client#PendingCodeAt")
	if n.http != nil {
		code, err = n.http.geth.PendingCodeAt(ctx, account)
		err = n.wrapHTTP(err)
	} else {
		code, err = n.ws.geth.PendingCodeAt(ctx, account)
		err = n.wrapWS(err)
	}

	logResult(lggr, err, "code", code)

	return
}

func (n *node) CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) (code []byte, err error) {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n)).With("account", account, "blockNumber", blockNumber)

	lggr.Debug("RPC call: evmclient.Client#CodeAt")
	if n.http != nil {
		code, err = n.http.geth.CodeAt(ctx, account, blockNumber)
		err = n.wrapHTTP(err)
	} else {
		code, err = n.ws.geth.CodeAt(ctx, account, blockNumber)
		err = n.wrapWS(err)
	}

	logResult(lggr, err, "code", code)

	return
}

func (n *node) EstimateGas(ctx context.Context, call ethereum.CallMsg) (gas uint64, err error) {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return 0, err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n)).With("call", call)

	lggr.Debug("RPC call: evmclient.Client#EstimateGas")
	if n.http != nil {
		gas, err = n.http.geth.EstimateGas(ctx, call)
		err = n.wrapHTTP(err)
	} else {
		gas, err = n.ws.geth.EstimateGas(ctx, call)
		err = n.wrapWS(err)
	}

	logResult(lggr, err, "gas", gas)

	return
}

func (n *node) SuggestGasPrice(ctx context.Context) (price *big.Int, err error) {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n))

	lggr.Debug("RPC call: evmclient.Client#SuggestGasPrice")
	if n.http != nil {
		price, err = n.http.geth.SuggestGasPrice(ctx)
		err = n.wrapHTTP(err)
	} else {
		price, err = n.ws.geth.SuggestGasPrice(ctx)
		err = n.wrapWS(err)
	}

	logResult(lggr, err, "price", price)

	return
}

func (n *node) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) (val []byte, err error) {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n)).With("msg", msg, "blockNumber", blockNumber)

	lggr.Debug("RPC call: evmclient.Client#CallContract")
	if n.http != nil {
		val, err = n.http.geth.CallContract(ctx, msg, blockNumber)
		err = n.wrapHTTP(err)
	} else {
		val, err = n.ws.geth.CallContract(ctx, msg, blockNumber)
		err = n.wrapWS(err)
	}

	logResult(lggr, err, "val", val)

	return

}

func (n *node) BlockByNumber(ctx context.Context, number *big.Int) (b *types.Block, err error) {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n)).With("number", number)

	lggr.Debug("RPC call: evmclient.Client#BlockByNumber")
	if n.http != nil {
		b, err = n.http.geth.BlockByNumber(ctx, number)
		err = n.wrapHTTP(err)
	} else {
		b, err = n.ws.geth.BlockByNumber(ctx, number)
		err = n.wrapWS(err)
	}

	logResult(lggr, err, "block", b)

	return
}

func (n *node) BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (balance *big.Int, err error) {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n)).With("account", account, "blockNumber", blockNumber)

	lggr.Debug("RPC call: evmclient.Client#BalanceAt")
	if n.http != nil {
		balance, err = n.http.geth.BalanceAt(ctx, account, blockNumber)
		err = n.wrapHTTP(err)
	} else {
		balance, err = n.ws.geth.BalanceAt(ctx, account, blockNumber)
		err = n.wrapWS(err)
	}

	logResult(lggr, err, "balance", balance)

	return
}

func (n *node) FilterLogs(ctx context.Context, q ethereum.FilterQuery) (l []types.Log, err error) {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n)).With("q", q)

	lggr.Debug("RPC call: evmclient.Client#FilterLogs")
	if n.http != nil {
		l, err = n.http.geth.FilterLogs(ctx, q)
		err = n.wrapHTTP(err)
	} else {
		l, err = n.ws.geth.FilterLogs(ctx, q)
		err = n.wrapWS(err)
	}

	logResult(lggr, err, "log", l)

	return
}

func (n *node) SubscribeFilterLogs(ctx context.Context, q ethereum.FilterQuery, ch chan<- types.Log) (sub ethereum.Subscription, err error) {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	lggr := n.newRqLggr("websocket").With("q", q)

	lggr.Debug("RPC call: evmclient.Client#SubscribeFilterLogs")
	sub, err = n.ws.geth.SubscribeFilterLogs(ctx, q, ch)
	err = n.wrapWS(err)

	logResult(lggr, err)

	return
}

func (n *node) SuggestGasTipCap(ctx context.Context) (tipCap *big.Int, err error) {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n))

	lggr.Debug("RPC call: evmclient.Client#SuggestGasTipCap")
	if n.http != nil {
		tipCap, err = n.http.geth.SuggestGasTipCap(ctx)
		err = n.wrapHTTP(err)
	} else {
		tipCap, err = n.ws.geth.SuggestGasTipCap(ctx)
		err = n.wrapWS(err)
	}

	logResult(lggr, err, "tipCap", tipCap)

	return
}

func (n *node) ChainID(ctx context.Context) (chainID *big.Int, err error) {
	ctx, cancel, err := n.wrapLiveCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	lggr := n.newRqLggr(switching(n))

	lggr.Debug("RPC call: evmclient.Client#ChainID")
	if n.http != nil {
		chainID, err = n.http.geth.ChainID(ctx)
		err = n.wrapHTTP(err)
	} else {
		chainID, err = n.ws.geth.ChainID(ctx)
		err = n.wrapWS(err)
	}

	logResult(lggr, err, "chainID", chainID)

	return
}

// newRqLggr generates a new logger with a unique request ID
func (n *node) newRqLggr(mode string) logger.Logger {
	return n.log.With(
		"requestID", uuid.NewV4(),
		"mode", mode,
	)
}

func logResult(lggr logger.Logger, err error, results ...interface{}) {
	if err == nil {
		lggr.Debugw("RPC call success", results...)
	} else {
		lggr.Debugw("RPC call failure", "err", err)
	}
}

func (n *node) wrapWS(err error) error {
	err = wrap(err, fmt.Sprintf("primary websocket (%s)", n.ws.uri.String()))
	if err != nil {
		n.log.Debugw("Call failed", "err", err)
	} else {
		n.log.Trace("Call succeeded")
	}
	return err
}

func (n *node) wrapHTTP(err error) error {
	err = wrap(err, fmt.Sprintf("primary http (%s)", n.http.uri.String()))
	if err != nil {
		n.log.Debugw("Call failed", "err", err)
	} else {
		n.log.Trace("Call succeeded")
	}
	return err
}

func wrap(err error, tp string) error {
	if err == nil {
		return nil
	}
	if errors.Cause(err).Error() == "context deadline exceeded" {
		err = errors.Wrap(err, "remote eth node timed out")
	}
	return errors.Wrapf(err, "%s call failed", tp)
}

// wrapLiveCtx adds a default timeout and combines with the node's master context
// which can be cancelled early.
//
// Returns error if node is not "alive".
//
// NOTE: We don't want to wrap all calls in a mutex lock via IfAlive or
// something similar because rpc calls can be slow and there are a lot
// of them: we might end up holding a read lock for a really long time
// through various overlapping requests, preventing state transitions.
//
// Instead, we check if the node is alive, and if so, copy the context pointer.
// If the node is marked dead during the request, the master context will be
// cancelled and the request will exit early.
func (n *node) wrapLiveCtx(parentCtx context.Context) (combinedCtx context.Context, cancel context.CancelFunc, err error) {
	// Need to wrap in mutex because state transition can cancel and replace the
	// context
	n.stateMu.RLock()
	if n.state != NodeStateAlive {
		err = errors.Errorf("cannot execute RPC call on node with state: %s", n.state)
		n.stateMu.RUnlock()
		return
	}
	nodeCtx := n.ctx
	n.stateMu.RUnlock()
	combinedCtx, cancel = wrapCtx(parentCtx, nodeCtx)
	return
}

func (n *node) wrapCtx(parentCtx context.Context) (combinedCtx context.Context, cancel context.CancelFunc) {
	nodeCtx := n.getCtx()
	combinedCtx, cancel = wrapCtx(parentCtx, nodeCtx)
	return
}

// getCtx wraps context access in the stateMu since state transitions can
// cancel and replace contexts
func (n *node) getCtx() context.Context {
	n.stateMu.RLock()
	defer n.stateMu.RUnlock()
	return n.ctx
}

func wrapCtx(parentCtx, nodeCtx context.Context) (combinedCtx context.Context, cancel context.CancelFunc) {
	combinedCtx, cancel = utils.CombinedContext(parentCtx, nodeCtx, queryTimeout)
	return
}

func (n *node) ctxWithDefaultTimeout() (context.Context, context.CancelFunc) {
	return DefaultQueryCtx(n.getCtx())
}

func switching(n *node) string {
	if n.http != nil {
		return "http"
	}
	return "websocket"
}

func (n *node) String() string {
	s := fmt.Sprintf("(primary)%s:%s", n.name, n.ws.uri.String())
	if n.http != nil {
		s = s + fmt.Sprintf(":%s", n.http.uri.String())
	}
	return s
}
