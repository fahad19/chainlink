package client

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/pkg/errors"
	evmtypes "github.com/smartcontractkit/chainlink/core/chains/evm/types"
)

// TODO: Rename this file to state management?

// This handles node lifecycle for the ALIVE state
// Should only be run ONCE per node, after a successful Dial
func (n *node) aliveLoop() {
	defer n.wg.Done()

	if state := n.State(); state != NodeStateAlive {
		panic(fmt.Sprintf("aliveLoop can only run for node in Alive state, got: %s", state))
	}

	// TODO: Rename this
	deadAfter := n.cfg.NodeNoNewHeadsThreshold()
	pollFailureThreshold := n.cfg.NodePollFailureThreshold()
	pollInterval := n.cfg.NodePollInterval()

	lggr := n.log.Named("Alive").With("deadAfter", deadAfter, "pollInterval", pollInterval, "pollFailureThreshold", pollFailureThreshold)
	lggr.Tracew("Alive loop starting", "nodeState", n.State())

	// TODO: Test this case
	var ch <-chan *evmtypes.Head
	var outOfSyncT *time.Ticker
	var outOfSyncTC <-chan time.Time
	var sub ethereum.Subscription
	if deadAfter > 0 {
		lggr.Debugw("Head liveness checking enabled", "deadAfterThreshold", deadAfter)
		var err error
		ch, sub, err = n.subscribeHeads()
		if err != nil {
			lggr.Errorw("Initial subscribe for liveness checking failed", "nodeState", n.State())
			n.declareUnreachable()
			return
		}
		defer func() {
			sub.Unsubscribe()
		}()
		outOfSyncT = time.NewTicker(deadAfter)
		defer outOfSyncT.Stop()
		outOfSyncTC = outOfSyncT.C
	} else {
		lggr.Debug("Head liveness checking disabled")
	}

	// TODO: Test this case
	var pollCh <-chan time.Time
	if pollFailureThreshold > 0 && pollInterval > 0 {
		lggr.Debug("Polling liveness checking enabled")
		pollT := time.NewTicker(pollInterval)
		defer pollT.Stop()
		pollCh = pollT.C
	} else {
		lggr.Debug("Polling liveness checking disabled")
	}

	var latestReceivedBlockNumber int64 = -1
	var pollFailures uint32

	for {
		select {
		case <-n.getCtx().Done():
			return
		case <-pollCh:
			var version string
			lggr.Tracew("Polling for version", "nodeState", n.State(), "pollFailures", pollFailures)
			ctx, cancel := context.WithTimeout(n.getCtx(), pollInterval)
			err := n.CallContext(ctx, &version, "eth_protocolVersion")
			cancel()
			if err != nil {
				// prevent overflow
				if pollFailures < math.MaxUint32 {
					pollFailures++
				}
				lggr.Warnw(fmt.Sprintf("Poll failure, node %s failed to respond properly", n.String()), "err", err, "pollFailures", pollFailures, "nodeState", n.State())
			} else {
				lggr.Tracew("Version poll successful", "nodeState", n.State(), "protocolVersion", version)
				pollFailures = 0
			}
			if pollFailures >= pollFailureThreshold {
				lggr.Errorw(fmt.Sprintf("Node failed to respond to %d consecutive polls", pollFailures), "pollFailures", pollFailures, "nodeState", n.State())
				n.declareUnreachable()
				return
			}
		case bh, open := <-ch:
			if !open {
				lggr.Errorw("Subscription channel unexpectedly closed", "nodeState", n.State())
				n.declareUnreachable()
				return
			}
			lggr.Tracew("Got head", "head", bh)
			if bh.Number > latestReceivedBlockNumber {
				lggr.Tracew("Got higher block number, resetting timer", "latestReceivedBlockNumber", latestReceivedBlockNumber, "blockNumber", bh.Number, "nodeState", n.State())
				latestReceivedBlockNumber = bh.Number
			} else {
				lggr.Tracew("Ignoring previously seen block number", "latestReceivedBlockNumber", latestReceivedBlockNumber, "blockNumber", bh.Number, "nodeState", n.State())
			}
			outOfSyncT.Reset(deadAfter)
		case err := <-sub.Err():
			lggr.Errorw("Subscription was terminated", "err", err, "nodeState", n.State())
			n.declareUnreachable()
			return
		case <-outOfSyncTC:
			// We haven't received a head on the channel for at least the
			// threshold amount of time, mark it broken
			lggr.Errorw(fmt.Sprintf("Node detected out of sync; no new heads received for %s (last head received was %v)", deadAfter, latestReceivedBlockNumber), "nodeState", n.State(), "latestReceivedBlockNumber", latestReceivedBlockNumber, "deadAfter", deadAfter)
			n.declareOutOfSync(latestReceivedBlockNumber)
			return
		}
	}
}

// subscribeHeads is used for internal liveness checking
func (n *node) subscribeHeads() (ch chan *evmtypes.Head, sub ethereum.Subscription, err error) {
	subCtx, cancel := n.ctxWithDefaultTimeout()
	defer cancel()
	ch = make(chan *evmtypes.Head)
	sub, err = n.EthSubscribe(subCtx, ch, "newHeads")
	err = errors.Wrap(err, "failed to subscribeHeads")
	return
}

// outOfSyncLoop takes an OutOfSync node and puts it back to live status if it
// receives a later head than one we have already seen
func (n *node) outOfSyncLoop(stuckAtBlockNumber int64) {
	defer n.wg.Done()

	if state := n.State(); state != NodeStateOutOfSync {
		panic(fmt.Sprintf("outOfSyncLoop can only run for node in OutOfSync state, got: %s", state))
	}

	diedAt := time.Now()

	lggr := n.log.Named("OutOfSync")
	lggr.Debugw("Trying to revive out-of-sync node", "nodeState", n.State())

	// Need to redial since out-of-sync nodes are automatically disconnected
	ctx, cancel := n.ctxWithDefaultTimeout()
	if err := n.dial(ctx); err != nil {
		cancel()
		lggr.Errorw("Failed to dial out-of-sync node", "nodeState", n.State())
		n.declareUnreachable()
		return
	}
	cancel()

	n.setState(NodeStateDialed)

	// Manually re-verify since out-of-sync nodes are automatically disconnected
	verifyCtx, cancel := n.ctxWithDefaultTimeout()
	if err := n.verify(verifyCtx); err != nil {
		cancel()
		n.log.Errorw(fmt.Sprintf("Failed to verify out-of-sync node: %v", err), "err", err)
		n.declareInvalidChainID()
		return
	}
	cancel()

	n.setState(NodeStateOutOfSync)

	lggr.Tracew("Successfully subscribed to heads feed on out-of-sync node", "stuckAtBlockNumber", stuckAtBlockNumber, "nodeState", n.State())

	ch := make(chan *evmtypes.Head)
	subCtx, cancel := n.ctxWithDefaultTimeout()
	// raw call here to bypass node state checking
	sub, err := n.ws.rpc.EthSubscribe(subCtx, ch, "newHeads")
	if err != nil {
		lggr.Errorw("Failed to subscribe heads on out-of-sync node", "nodeState", n.State(), "err", err)
		n.declareUnreachable()
		return
	}
	defer sub.Unsubscribe()

	nodeCtx := n.getCtx()

	for {
		select {
		case <-nodeCtx.Done():
			return
		case head, open := <-ch:
			if !open {
				lggr.Error("Subscription channel unexpectedly closed", "nodeState", n.State())
				n.declareUnreachable()
				return
			}
			if head.Number > stuckAtBlockNumber {
				// unstuck! flip back into alive loop
				lggr.Infow(fmt.Sprintf("Received new block for node %s. Node was offline for %s", n.String(), time.Since(diedAt)), "latestReceivedBlockNumber", head.Number, "nodeState", n.State())
				n.declareInSync()
				return
			}
			lggr.Tracew("Received previously seen block for node, waiting for new block before marking as live again", "stuckAtBlockNumber", stuckAtBlockNumber, "blockNumber", head.Number, "nodeState", n.State())
		case err := <-sub.Err():
			lggr.Errorw("Subscription was terminated", "nodeState", n.State(), "err", err)
			n.declareUnreachable()
			return
		}
	}
}

// dialRetryInterval controls how often we try to redial an unreachable node
const dialRetryInterval = 5 * time.Second

func (n *node) unreachableLoop() {
	defer n.wg.Done()

	if state := n.State(); state != NodeStateUnreachable {
		panic(fmt.Sprintf("unreachableLoop can only run for node in Unreachable state, got: %s", state))
	}

	unreachableAt := time.Now()

	lggr := n.log.Named("Unreachable")
	lggr.Debugw("Trying to revive unreachable node", "nodeState", n.State())

	ticker := time.NewTicker(dialRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.getCtx().Done():
			return
		case <-ticker.C:
			lggr.Tracew("Trying to re-dial node", "nodeState", n.State())

			dialCtx, cancel := n.ctxWithDefaultTimeout()
			err := n.dial(dialCtx)
			cancel()
			if err != nil {
				lggr.Errorw(fmt.Sprintf("Failed to redial node; still unreachable: %v", err), "err", err, "nodeState", n.State())
				continue
			}

			n.setState(NodeStateDialed)

			verifyCtx, cancel := n.ctxWithDefaultTimeout()
			err = n.verify(verifyCtx)
			cancel()
			if errors.Is(err, errInvalidChainID) {
				lggr.Errorw("Failed to redial node; EVM Node has the wrong chain ID", "err", err)
				n.declareInvalidChainID()
				return
			} else if err != nil {
				lggr.Errorw(fmt.Sprintf("Failed to redial node; verify failed: %v", err), "err", err)
				n.declareUnreachable()
				return
			}

			lggr.Infow(fmt.Sprintf("Successfully redialled and verified node %s. Node was offline for %s", n.String(), time.Since(unreachableAt)), "nodeState", n.State())
			n.declareAlive()
			return
		}
	}
}

func (n *node) invalidChainIDLoop() {
	defer n.wg.Done()

	if state := n.State(); state != NodeStateInvalidChainID {
		panic(fmt.Sprintf("invalidChainIDLoop can only run for node in InvalidChainID state, got: %s", state))
	}

	invalidAt := time.Now()

	lggr := n.log.Named("InvalidChainID")
	lggr.Debugw(fmt.Sprintf("Periodically re-checking node %s with invalid chain ID", n.String()), "nodeState", n.State())

	ticker := time.NewTicker(dialRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.getCtx().Done():
			return
		case <-ticker.C:
			verifyCtx, cancel := n.ctxWithDefaultTimeout()
			err := n.verify(verifyCtx)
			cancel()
			if errors.Is(err, errInvalidChainID) {
				lggr.Errorw("Failed to verify node; EVM Node has the wrong chain ID", "err", err)
				continue
			} else if err != nil {
				lggr.Errorw(fmt.Sprintf("Unexpected error while verifying node chain ID; %v", err), "err", err)
				n.declareUnreachable()
				return
			}
			lggr.Infow(fmt.Sprintf("Successfully verified node. Node was offline for %s", time.Since(invalidAt)), "nodeState", n.State())
			n.declareAlive()
			return
		}
	}
}
