package evidence

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/celestiaorg/go-libp2p-messenger"
	"github.com/celestiaorg/go-libp2p-messenger/serde"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	clist "github.com/tendermint/tendermint/internal/libs/clist"
	tmsync "github.com/tendermint/tendermint/internal/libs/sync"
	"github.com/tendermint/tendermint/internal/p2p"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/libs/service"
	tmproto "github.com/tendermint/tendermint/proto/tendermint/types"
	"github.com/tendermint/tendermint/types"
)

var (
	_ service.Service = (*Reactor)(nil)

	// ChannelShims contains a map of ChannelDescriptorShim objects, where each
	// object wraps a reference to a legacy p2p ChannelDescriptor and the corresponding
	// p2p proto.Message the new p2p Channel is responsible for handling.
	//
	//
	// TODO: Remove once p2p refactor is complete.
	// ref: https://github.com/tendermint/tendermint/issues/5670
	ChannelShims = map[p2p.ChannelID]*p2p.ChannelDescriptorShim{
		EvidenceChannel: {
			MsgType: new(tmproto.EvidenceList),
			Descriptor: &p2p.ChannelDescriptor{
				ID:                  byte(EvidenceChannel),
				Priority:            6,
				RecvMessageCapacity: maxMsgSize,
				RecvBufferCapacity:  32,
				MaxSendBytes:        400,
			},
		},
	}
)

const (
	EvidenceChannel = p2p.ChannelID(0x38)

	maxMsgSize = 1048576 // 1MB TODO make it configurable

	// broadcast all uncommitted evidence this often. This sets when the reactor
	// goes back to the start of the list and begins sending the evidence again.
	// Most evidence should be committed in the very next block that is why we wait
	// just over the block production rate before sending evidence again.
	broadcastEvidenceIntervalS = 10
)

const Protocol protocol.ID = "/evidence/0.1.0"

// Reactor handles evpool evidence broadcasting amongst peers.
type Reactor struct {
	service.BaseService

	evpool *Pool

	peerWG sync.WaitGroup

	mtx          tmsync.Mutex
	peerRoutines map[peer.ID]*tmsync.Closer

	msgr   *msngr.Messenger
	ctx    context.Context
	cancel context.CancelFunc
}

// NewReactor returns a reference to a new evidence reactor, which implements the
// service.Service interface. It accepts a p2p Channel dedicated for handling
// envelopes with EvidenceList messages.
func NewReactor(
	logger log.Logger,
	host host.Host,
	evpool *Pool,
) (*Reactor, error) {
	msgr, err := msngr.New(host,
		msngr.WithProtocols(Protocol),
		msngr.WithMessageType(&tmproto.EvidenceList{}),
	)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := &Reactor{
		evpool:       evpool,
		peerRoutines: make(map[peer.ID]*tmsync.Closer),
		msgr:         msgr,
		ctx:          ctx,
		cancel:       cancel,
	}

	r.BaseService = *service.NewBaseService(logger, "Evidence", r)
	return r, nil
}

// OnStart starts separate go routines for each p2p Channel and listens for
// envelopes on each. In addition, it also listens for peer updates and handles
// messages on that p2p channel accordingly. The caller must be sure to execute
// OnStop to ensure the outbound p2p Channels are closed. No error is returned.
func (r *Reactor) OnStart() error {
	go r.processEvidenceCh()
	go r.processPeerUpdates()

	return nil
}

// OnStop stops the reactor by signaling to all spawned goroutines to exit and
// blocking until they all exit.
func (r *Reactor) OnStop() {
	r.mtx.Lock()
	for _, c := range r.peerRoutines {
		c.Close()
	}
	r.mtx.Unlock()

	// Wait for all spawned peer evidence broadcasting goroutines to gracefully
	// exit.
	r.peerWG.Wait()

	// Close closeCh to signal to all spawned goroutines to gracefully exit. All
	// p2p Channels should execute Close().
	r.cancel()

	r.msgr.Close()
}

// handleEvidenceMessage handles envelopes sent from peers on the EvidenceChannel.
// It returns an error only if the Envelope.Message is unknown for this channel
// or if the given evidence is invalid. This should never be called outside of
// handleMessage.
func (r *Reactor) handleEvidenceMessage(from peer.ID, msg serde.Message) error {
	logger := r.Logger.With("from", from.ShortString())

	switch msg := msg.(type) {
	case *tmproto.EvidenceList:
		// TODO: Refactor the Evidence type to not contain a list since we only ever
		// send and receive one piece of evidence at a time. Or potentially consider
		// batching evidence.
		//
		// see: https://github.com/tendermint/tendermint/issues/4729
		for i := 0; i < len(msg.Evidence); i++ {
			ev, err := types.EvidenceFromProto(&msg.Evidence[i])
			if err != nil {
				logger.Error("failed to convert evidence", "err", err)
				continue
			}

			if err := r.evpool.AddEvidence(ev); err != nil {
				// If we're given invalid evidence by the peer, notify the router that
				// we should remove this peer by returning an error.
				if _, ok := err.(*types.ErrInvalidEvidence); ok {
					return err
				}
			}
		}

	default:
		return fmt.Errorf("received unknown message: %T", msg)
	}

	return nil
}

// handleMessage handles an Envelope sent from a peer on a specific p2p Channel.
// It will handle errors and any possible panics gracefully. A caller can handle
// any error returned by sending a PeerError on the respective channel.
func (r *Reactor) handleMessage(from peer.ID, msg serde.Message) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("panic in processing message: %v", e)
			r.Logger.Error(
				"recovering from processing message panic",
				"err", err,
				"stack", string(debug.Stack()),
			)
		}
	}()

	r.Logger.Debug("received message", "message", msg, "from", from.ShortString())

	return r.handleEvidenceMessage(from, msg)
}

// processEvidenceCh implements a blocking event loop where we listen for p2p
// Envelope messages from the evidenceCh.
func (r *Reactor) processEvidenceCh() {
	for {
		msg, from, err := r.msgr.Receive(r.ctx)
		if err != nil { // only returned when context is canceled
			r.Logger.Debug("stopped listening on evidence channel; closing...")
			return
		}

		if err := r.handleMessage(from, msg); err != nil {
			r.Logger.Error(
				"failed to process message",
				"protocol", Protocol, "from", from.ShortString(), "err", err,
			)

			// TODO: Is centralized error handling reasonable?
			// r.evidenceCh.Error <- p2p.PeerError{
			// 	NodeID: envelope.From,
			// 	Err:    err,
			// }
		}
	}
}

// processPeerUpdate processes a PeerUpdate. For new or live peers it will check
// if an evidence broadcasting goroutine needs to be started. For down or
// removed peers, it will check if an evidence broadcasting goroutine
// exists and signal that it should exit.
//
// FIXME: The peer may be behind in which case it would simply ignore the
// evidence and treat it as invalid. This would cause the peer to disconnect.
// The peer may also receive the same piece of evidence multiple times if it
// connects/disconnects frequently from the broadcasting peer(s).
//
// REF: https://github.com/tendermint/tendermint/issues/4727
func (r *Reactor) processPeerUpdate(peerUpdate msngr.PeerEvent) {
	r.Logger.Debug("received peer update",
		"peer", peerUpdate.ID.ShortString(),
		"status", peerUpdate.State,
	)

	r.mtx.Lock()
	defer r.mtx.Unlock()

	switch peerUpdate.State {
	case network.Connected:
		// Do not allow starting new evidence broadcast loops after reactor shutdown
		// has been initiated. This can happen after we've manually closed all
		// peer broadcast loops and closed r.closeCh, but the router still sends
		// in-flight peer updates.
		if !r.IsRunning() {
			return
		}

		// Check if we've already started a goroutine for this peer, if not we create
		// a new done channel so we can explicitly close the goroutine if the peer
		// is later removed, we increment the waitgroup so the reactor can stop
		// safely, and finally start the goroutine to broadcast evidence to that peer.
		_, ok := r.peerRoutines[peerUpdate.ID]
		if !ok {
			closer := tmsync.NewCloser()

			r.peerRoutines[peerUpdate.ID] = closer
			r.peerWG.Add(1)
			go r.broadcastEvidenceLoop(peerUpdate.ID, closer)
		}

	case network.NotConnected:
		// Check if we've started an evidence broadcasting goroutine for this peer.
		// If we have, we signal to terminate the goroutine via the channel's closure.
		// This will internally decrement the peer waitgroup and remove the peer
		// from the map of peer evidence broadcasting goroutines.
		closer, ok := r.peerRoutines[peerUpdate.ID]
		if ok {
			closer.Close()
		}
	}
}

// processPeerUpdates initiates a blocking process where we listen for and handle
// PeerUpdate messages. When the reactor is stopped, we will catch the signal and
// close the p2p PeerUpdatesCh gracefully.
func (r *Reactor) processPeerUpdates() {
	for {
		select {
		case peerUpdate := <-r.msgr.Events():
			r.processPeerUpdate(peerUpdate)

		case <-r.ctx.Done():
			r.Logger.Debug("stopped listening on peer updates channel; closing...")
			return
		}
	}
}

// broadcastEvidenceLoop starts a blocking process that continuously reads pieces
// of evidence off of a linked-list and sends the evidence in a p2p Envelope to
// the given peer by ID. This should be invoked in a goroutine per unique peer
// ID via an appropriate PeerUpdate. The goroutine can be signaled to gracefully
// exit by either explicitly closing the provided doneCh or by the reactor
// signaling to stop.
//
// TODO: This should be refactored so that we do not blindly gossip evidence
// that the peer has already received or may not be ready for.
//
// REF: https://github.com/tendermint/tendermint/issues/4727
func (r *Reactor) broadcastEvidenceLoop(peerID peer.ID, closer *tmsync.Closer) {
	var next *clist.CElement

	defer func() {
		r.mtx.Lock()
		delete(r.peerRoutines, peerID)
		r.mtx.Unlock()

		r.peerWG.Done()

		if e := recover(); e != nil {
			r.Logger.Error(
				"recovering from broadcasting evidence loop",
				"err", e,
				"stack", string(debug.Stack()),
			)
		}
	}()

	for {
		// This happens because the CElement we were looking at got garbage
		// collected (removed). That is, .NextWaitChan() returned nil. So we can go
		// ahead and start from the beginning.
		if next == nil {
			select {
			case <-r.evpool.EvidenceWaitChan(): // wait until next evidence is available
				if next = r.evpool.EvidenceFront(); next == nil {
					continue
				}

			case <-closer.Done():
				// The peer is marked for removal via a PeerUpdate as the doneCh was
				// explicitly closed to signal we should exit.
				return

			case <-r.ctx.Done():
				// The reactor has signaled that we are stopped and thus we should
				// implicitly exit this peer's goroutine.
				return
			}
		}

		ev := next.Value.(types.Evidence)
		evProto, err := types.EvidenceToProto(ev)
		if err != nil {
			panic(fmt.Errorf("failed to convert evidence: %w", err))
		}

		// Send the evidence to the corresponding peer. Note, the peer may be behind
		// and thus would not be able to process the evidence correctly. Also, the
		// peer may receive this piece of evidence multiple times if it added and
		// removed frequently from the broadcasting peer.
		r.msgr.Send(r.ctx, &tmproto.EvidenceList{
			Evidence: []tmproto.Evidence{*evProto},
		}, peerID)
		r.Logger.Debug("gossiped evidence to peer", "evidence", ev, "peer", peerID.ShortString())

		select {
		case <-time.After(time.Second * broadcastEvidenceIntervalS):
			// start from the beginning after broadcastEvidenceIntervalS seconds
			next = nil

		case <-next.NextWaitChan():
			next = next.Next()

		case <-closer.Done():
			// The peer is marked for removal via a PeerUpdate as the doneCh was
			// explicitly closed to signal we should exit.
			return

		case <-r.ctx.Done():
			// The reactor has signaled that we are stopped and thus we should
			// implicitly exit this peer's goroutine.
			return
		}
	}
}
