package ackhandler

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	congestionExt "github.com/olicesx/quic-go/congestion"
	"github.com/olicesx/quic-go/internal/protocol"
	"github.com/olicesx/quic-go/internal/utils"
	"github.com/olicesx/quic-go/internal/wire"
	"github.com/stretchr/testify/require"
)

type snapshotCongestionControl struct {
	maybeExitEntered chan struct{}
	maybeExitRelease chan struct{}
	maybeExitOnce    sync.Once

	acked    atomic.Int32
	lost     atomic.Int32
	extended atomic.Int32
}

func (c *snapshotCongestionControl) SetRTTStatsProvider(congestionExt.RTTStatsProvider) {}
func (c *snapshotCongestionControl) TimeUntilSend(congestionExt.ByteCount) time.Time {
	return time.Time{}
}
func (c *snapshotCongestionControl) HasPacingBudget(time.Time) bool { return true }
func (c *snapshotCongestionControl) OnPacketSent(time.Time, congestionExt.ByteCount, congestionExt.PacketNumber, congestionExt.ByteCount, bool) {
}
func (c *snapshotCongestionControl) CanSend(congestionExt.ByteCount) bool { return true }
func (c *snapshotCongestionControl) MaybeExitSlowStart() {
	if c.maybeExitEntered == nil {
		return
	}
	c.maybeExitOnce.Do(func() { close(c.maybeExitEntered) })
	<-c.maybeExitRelease
}
func (c *snapshotCongestionControl) OnPacketAcked(congestionExt.PacketNumber, congestionExt.ByteCount, congestionExt.ByteCount, time.Time) {
	c.acked.Add(1)
}
func (c *snapshotCongestionControl) OnCongestionEvent(congestionExt.PacketNumber, congestionExt.ByteCount, congestionExt.ByteCount) {
	c.lost.Add(1)
}
func (c *snapshotCongestionControl) OnCongestionEventEx(congestionExt.ByteCount, time.Time, []congestionExt.AckedPacketInfo, []congestionExt.LostPacketInfo) {
	c.extended.Add(1)
}
func (c *snapshotCongestionControl) OnRetransmissionTimeout(bool)                 {}
func (c *snapshotCongestionControl) SetMaxDatagramSize(congestionExt.ByteCount)   {}
func (c *snapshotCongestionControl) InSlowStart() bool                            { return false }
func (c *snapshotCongestionControl) InRecovery() bool                             { return false }
func (c *snapshotCongestionControl) GetCongestionWindow() congestionExt.ByteCount { return 1 << 20 }

func TestReceivedAckUsesSingleCongestionSnapshot(t *testing.T) {
	var rttStats utils.RTTStats
	h := newSentPacketHandler(
		0,
		1200,
		&rttStats,
		true,
		false,
		protocol.PerspectiveClient,
		nil,
		utils.DefaultLogger,
	)

	oldCC := &snapshotCongestionControl{
		maybeExitEntered: make(chan struct{}),
		maybeExitRelease: make(chan struct{}),
	}
	h.SetCongestionControl(oldCC)

	var packets packetTracker
	now := time.Now()
	packetNumbers := make([]protocol.PacketNumber, 0, 4)
	for i := range 4 {
		pn := h.PopPacketNumber(protocol.Encryption1RTT)
		packetNumbers = append(packetNumbers, pn)
		h.SentPacket(
			now.Add(time.Duration(i)*time.Millisecond),
			pn,
			protocol.InvalidPacketNumber,
			nil,
			[]Frame{packets.NewPingFrame(pn)},
			protocol.Encryption1RTT,
			protocol.ECNNon,
			1200,
			false,
		)
	}

	ackDone := make(chan error, 1)
	go func() {
		_, err := h.ReceivedAck(
			&wire.AckFrame{AckRanges: ackRanges(packetNumbers[3])},
			protocol.Encryption1RTT,
			now.Add(10*time.Millisecond),
		)
		ackDone <- err
	}()

	select {
	case <-oldCC.maybeExitEntered:
	case <-time.After(time.Second):
		t.Fatal("ReceivedAck did not reach the snapshotted controller")
	}

	newCC := &snapshotCongestionControl{}
	h.SetCongestionControl(newCC)
	close(oldCC.maybeExitRelease)

	select {
	case err := <-ackDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("ReceivedAck did not finish")
	}

	require.Equal(t, int32(1), oldCC.acked.Load())
	require.Positive(t, oldCC.lost.Load())
	require.Equal(t, int32(1), oldCC.extended.Load())
	require.Zero(t, newCC.acked.Load())
	require.Zero(t, newCC.lost.Load())
	require.Zero(t, newCC.extended.Load())
}
