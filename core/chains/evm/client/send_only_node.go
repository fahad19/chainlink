package client

import (
	"context"
	"fmt"
	"math/big"
	"net/url"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/pkg/errors"
	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/utils"
)

//go:generate mockery --name SendOnlyNode --output ../mocks/ --case=underscore

// SendOnlyNode represents one ethereum node used as a sendonly
type SendOnlyNode interface {
	Start(context.Context) error
	Close()

	ChainID(ctx context.Context) (chainID *big.Int, err error)

	SendTransaction(ctx context.Context, tx *types.Transaction) error
	BatchCallContext(ctx context.Context, b []rpc.BatchElem) error

	String() string
}

// It only supports sending transactions
// It must a http(s) url
type sendOnlyNode struct {
	uri     url.URL
	rpc     *rpc.Client
	geth    *ethclient.Client
	log     logger.Logger
	dialed  bool
	name    string
	chainID *big.Int
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewSendOnlyNode(lggr logger.Logger, httpuri url.URL, name string) SendOnlyNode {
	s := new(sendOnlyNode)
	s.name = name
	s.log = lggr.Named("SendOnlyNode").Named(name).With(
		"nodeTier", "sendonly",
	)
	s.uri = httpuri
	return s
}

// Start setups up and verifies the sendonly node
// Should only be called once in a node's lifecycle
func (s *sendOnlyNode) Start(startCtx context.Context) error {
	s.log.Debugw("evmclient.Client#Dial(...)")
	if s.dialed {
		panic("evmclient.Client.Dial(...) should only be called once during the node's lifetime.")
	}

	uri := s.uri.String()
	rpc, err := rpc.DialHTTP(uri)
	if err != nil {
		return errors.Wrapf(err, "failed to dial secondary client: %v", uri)
	}
	s.dialed = true
	s.rpc = rpc
	s.geth = ethclient.NewClient(rpc)

	if err := s.verify(startCtx); err != nil {
		return errors.Wrap(err, "failed to verify sendonly node")
	}

	// NOTE: Deliberately do not use startCtx here, this is background process
	s.ctx, s.cancel = context.WithCancel(context.Background())
	return nil
}

func (s *sendOnlyNode) Close() {
	s.cancel()
}

func (s sendOnlyNode) SendTransaction(parentCtx context.Context, tx *types.Transaction) error {
	s.log.Debugw("evmclient.Client#SendTransaction(...)",
		"tx", tx,
	)
	ctx, cancel := s.wrapCtx(parentCtx)
	defer cancel()
	return s.wrap(s.geth.SendTransaction(ctx, tx))
}

func (s sendOnlyNode) BatchCallContext(parentCtx context.Context, b []rpc.BatchElem) error {
	s.log.Debugw("evmclient.Client#BatchCall(...)",
		"nBatchElems", len(b),
	)
	ctx, cancel := s.wrapCtx(parentCtx)
	defer cancel()
	return s.wrap(s.rpc.BatchCallContext(ctx, b))
}

func (s sendOnlyNode) ChainID(parentCtx context.Context) (chainID *big.Int, err error) {
	s.log.Debugw("evmclient.Client#ChainID(...)")
	ctx, cancel := s.wrapCtx(parentCtx)
	defer cancel()
	chainID, err = s.geth.ChainID(ctx)
	err = s.wrap(err)
	return
}

func (s sendOnlyNode) wrap(err error) error {
	return wrap(err, fmt.Sprintf("sendonly http (%s)", s.uri.String()))
}

func (s sendOnlyNode) String() string {
	return fmt.Sprintf("(secondary)%s:%s", s.name, s.uri.String())
}

func (s sendOnlyNode) verify(ctx context.Context) (err error) {
	if chainID, err := s.ChainID(ctx); err != nil {
		return errors.Wrap(err, "failed to verify chain ID")
	} else if chainID.Cmp(s.chainID) != 0 {
		return errors.Errorf(
			"sendonly rpc ChainID doesn't match local chain ID: RPC ID=%s, local ID=%s, node name=%s",
			chainID.String(),
			s.chainID.String(),
			s.name,
		)
	}
	return nil
}

func (s sendOnlyNode) wrapCtx(parentCtx context.Context) (combinedCtx context.Context, cancel context.CancelFunc) {
	combinedCtx, cancel = utils.CombinedContext(parentCtx, s.ctx, queryTimeout)
	return
}
