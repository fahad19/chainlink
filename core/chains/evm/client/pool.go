package client

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/pkg/errors"
	"go.uber.org/atomic"

	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/utils"
)

// Pool represents an abstraction over one or more primary nodes
// It is responsible for liveness checking and balancing queries across live nodes
type Pool struct {
	utils.StartStopOnce
	nodes           []Node
	sendonlys       []SendOnlyNode
	chainID         *big.Int
	roundRobinCount atomic.Uint32
	logger          logger.Logger

	chStop chan struct{}
	wg     sync.WaitGroup
}

func NewPool(logger logger.Logger, nodes []Node, sendonlys []SendOnlyNode, chainID *big.Int) *Pool {
	if chainID == nil {
		panic("chainID is required")
	}
	p := &Pool{
		utils.StartStopOnce{},
		nodes,
		sendonlys,
		chainID,
		atomic.Uint32{},
		logger.Named("Pool").With("evmChainID", chainID.String()),
		make(chan struct{}),
		sync.WaitGroup{},
	}
	return p
}

// Dial starts every node in the pool
func (p *Pool) Dial(ctx context.Context) error {
	return p.StartOnce("Pool", func() (merr error) {
		if len(p.nodes) == 0 {
			return errors.Errorf("no available nodes for chain %s", p.chainID.String())
		}
		for _, n := range p.nodes {
			// node will handle its own redialing and automatic recovery
			n.Start(ctx)
		}
		for _, s := range p.sendonlys {
			// TODO: Deal with sendonly nodes state?
			// See: https://app.shortcut.com/chainlinklabs/story/8403/multiple-primary-geth-nodes-with-failover-load-balancer-part-2
			err := s.Start(ctx)
			if err != nil {
				return err
			}
		}
		p.wg.Add(1)
		go p.runLoop()

		return nil
	})
}

func (p *Pool) runLoop() {
	defer p.wg.Done()

	// FIXME: Use config for this?
	monitor := time.NewTicker(time.Second)
	// monitor := time.NewTicker(time.Minute)

	type nodeWithState struct {
		Node  string
		State string
	}

	for {
		select {
		case <-monitor.C:
			// TODO: Add prometheus, more monitoring etc in here
			var liveNodes, deadNodes int
			nodeStates := make([]nodeWithState, len(p.nodes))
			for i, n := range p.nodes {
				state := n.State()
				nodeStates[i] = nodeWithState{n.String(), state.String()}
				if state == NodeStateAlive {
					liveNodes++
				} else {
					deadNodes++
				}
			}
			total := liveNodes + deadNodes
			p.logger.Tracew(fmt.Sprintf("Pool state: %d/%[1]d nodes live and %d/%[1]d nodes dead", liveNodes, total, deadNodes), "nodeStates", nodeStates)
			if total == deadNodes {
				p.logger.Criticalw(fmt.Sprintf("No EVM primary nodes available: 0/%d nodes are alive", total), "nodeStates", nodeStates)
			} else if deadNodes > 0 {
				p.logger.Errorw(fmt.Sprintf("At least one EVM primary node is dead: %d/%d nodes are alive", liveNodes, total), "nodeStates", nodeStates)
			}
		case <-p.chStop:
			return
		}
	}
}

// Close tears down the pool and closes all nodes
func (p *Pool) Close() {
	//nolint:errcheck
	p.StopOnce("Pool", func() error {
		close(p.chStop)
		p.wg.Wait()

		var closeWg sync.WaitGroup
		closeWg.Add(len(p.nodes))
		for _, n := range p.nodes {
			go func() {
				defer closeWg.Done()
				n.Close()
			}()
		}
		closeWg.Add(len(p.sendonlys))
		for _, s := range p.sendonlys {
			go func() {
				defer closeWg.Done()
				s.Close()
			}()
		}
		closeWg.Wait()
		return nil
	})
}

func (p *Pool) ChainID() *big.Int {
	return p.chainID
}

// TODO: Handle case where all nodes are out-of-sync
func (p *Pool) getRoundRobin() Node {
	nodes := p.liveNodes()
	nNodes := len(nodes)
	if nNodes == 0 {
		// TODO: No nodes available should be logging at CRIT
		return &erroringNode{errMsg: fmt.Sprintf("no live nodes available for chain %s", p.chainID.String())}
	}

	// NOTE: Inc returns the number after addition, so we must -1 to get the "current" counter
	count := p.roundRobinCount.Inc() - 1
	idx := int(count % uint32(nNodes))

	return nodes[idx]
}

func (p *Pool) liveNodes() (liveNodes []Node) {
	for _, n := range p.nodes {
		if n.State() == NodeStateAlive {
			liveNodes = append(liveNodes, n)
		}
	}
	return
}

func (p *Pool) CallContext(ctx context.Context, result interface{}, method string, args ...interface{}) error {
	return p.getRoundRobin().CallContext(ctx, result, method, args...)
}

func (p *Pool) BatchCallContext(ctx context.Context, b []rpc.BatchElem) error {
	return p.getRoundRobin().BatchCallContext(ctx, b)
}

// Wrapped Geth client methods
func (p *Pool) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	main := p.getRoundRobin()
	var all []SendOnlyNode
	for _, n := range p.nodes {
		all = append(all, n)
	}
	all = append(all, p.sendonlys...)
	for _, n := range all {
		if n == main {
			// main node is used at the end for the return value
			continue
		}
		// Parallel send to all other nodes with ignored return value
		wg.Add(1)
		go func(n SendOnlyNode) {
			defer wg.Done()
			err := NewSendError(n.SendTransaction(ctx, tx))
			p.logger.Debugw("Sendonly node sent transaction", "name", n.String(), "tx", tx, "err", err)
			if err == nil || err.IsNonceTooLowError() || err.IsTransactionAlreadyInMempool() {
				// Nonce too low or transaction known errors are expected since
				// the primary SendTransaction may well have succeeded already
				return
			}

			p.logger.Warnw("Eth client returned error", "name", n.String(), "err", err, "tx", tx)
		}(n)
	}

	return main.SendTransaction(ctx, tx)
}

func (p *Pool) PendingCodeAt(ctx context.Context, account common.Address) ([]byte, error) {
	return p.getRoundRobin().PendingCodeAt(ctx, account)
}

func (p *Pool) PendingNonceAt(ctx context.Context, account common.Address) (uint64, error) {
	return p.getRoundRobin().PendingNonceAt(ctx, account)
}

func (p *Pool) NonceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (uint64, error) {
	return p.getRoundRobin().NonceAt(ctx, account, blockNumber)
}

func (p *Pool) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	return p.getRoundRobin().TransactionReceipt(ctx, txHash)
}

func (p *Pool) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	return p.getRoundRobin().BlockByNumber(ctx, number)
}

func (p *Pool) BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error) {
	return p.getRoundRobin().BalanceAt(ctx, account, blockNumber)
}

func (p *Pool) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	return p.getRoundRobin().FilterLogs(ctx, q)
}

func (p *Pool) SubscribeFilterLogs(ctx context.Context, q ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	return p.getRoundRobin().SubscribeFilterLogs(ctx, q, ch)
}

func (p *Pool) EstimateGas(ctx context.Context, call ethereum.CallMsg) (uint64, error) {
	return p.getRoundRobin().EstimateGas(ctx, call)
}

func (p *Pool) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	return p.getRoundRobin().SuggestGasPrice(ctx)
}

func (p *Pool) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	return p.getRoundRobin().CallContract(ctx, msg, blockNumber)
}

func (p *Pool) CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error) {
	return p.getRoundRobin().CodeAt(ctx, account, blockNumber)
}

// bind.ContractBackend methods
func (p *Pool) HeaderByNumber(ctx context.Context, n *big.Int) (*types.Header, error) {
	return p.getRoundRobin().HeaderByNumber(ctx, n)
}

func (p *Pool) SuggestGasTipCap(ctx context.Context) (*big.Int, error) {
	return p.getRoundRobin().SuggestGasTipCap(ctx)
}

func (p *Pool) EthSubscribe(ctx context.Context, channel interface{}, args ...interface{}) (ethereum.Subscription, error) {
	return p.getRoundRobin().EthSubscribe(ctx, channel, args...)
}
