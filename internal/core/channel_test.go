package core

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/jylc/nijigen-queue/internal/message"
	"github.com/jylc/nijigen-queue/internal/pb"
	"github.com/panjf2000/gnet"
	"github.com/panjf2000/gnet/pool/goroutine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newConn() net.Conn {
	conn, err := net.DialTCP("tcp", nil, &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: 6789,
	})
	if err != nil {
		panic(err)
	}
	return conn
}

type testServer struct {
	*gnet.EventServer
	tester       *testing.T
	network      string
	addr         string
	multicore    bool
	async        bool
	nclients     int
	started      int32
	connected    int32
	disconnected int32
	clientActive int32
	cb           func(frame []byte, conn gnet.Conn)
	workerPool   *goroutine.Pool
}

func (s *testServer) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	atomic.AddInt32(&s.connected, 1)
	conn := &NQConn{
		Conn: c,
		ID:   int64(s.connected),
	}
	c.SetContext(conn)
	fmt.Println("open a connection")
	out = []byte("sweetness\r\n")
	require.NotNil(s.tester, conn.LocalAddr(), "nil local addr")
	require.NotNil(s.tester, conn.RemoteAddr(), "nil remote addr")
	return
}
func (s *testServer) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	ctx := c.Context()
	if ctx != nil {
		if err, ok := ctx.(error); ok {
			panic(err)
		}
	}
	conn := c.Context()
	require.Equal(s.tester, c.Context(), conn, "invalid context")
	fmt.Println("close a connection")
	atomic.AddInt32(&s.disconnected, 1)
	if atomic.LoadInt32(&s.connected) == atomic.LoadInt32(&s.disconnected) &&
		atomic.LoadInt32(&s.disconnected) == int32(s.nclients) {
		action = gnet.Shutdown
	}
	return
}

func (s *testServer) React(frame []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	conn := c.Context().(*NQConn)
	fmt.Println("handle a connection", conn.RemoteAddr())
	s.cb(frame, conn)
	if s.async {
		if frame != nil {
			data := append([]byte{}, frame...)
			_ = s.workerPool.Submit(func() {
				_ = conn.AsyncWrite(data)
			})
		}
		return
	}
	out = frame
	c.SetContext(conn)
	return
}

func testServe(t *testing.T, network, addr string, reuseport, multicore, async bool, nclients int, lb gnet.LoadBalancing, cb func(frame []byte, conn gnet.Conn)) {
	ts := &testServer{
		tester:     t,
		network:    network,
		addr:       addr,
		multicore:  multicore,
		async:      async,
		nclients:   nclients,
		workerPool: nil,
		cb:         cb,
	}

	err := gnet.Serve(ts,
		network+"://"+addr,
		gnet.WithLockOSThread(async),
		gnet.WithMulticore(multicore),
		gnet.WithReusePort(reuseport),
		gnet.WithTicker(true),
		gnet.WithTCPKeepAlive(time.Minute*1),
		gnet.WithTCPNoDelay(gnet.TCPDelay),
		gnet.WithLoadBalancing(lb))
	assert.NoError(t, err)
}

func TestNewChannel(t *testing.T) {
	nq := NewNQ()
	assert.NotNil(t, nq)
	ch := NewChannel("channel-1", nq)
	assert.NotNil(t, ch)
}

func TestChannel_GetMsg(t *testing.T) {
	nq := NewNQ()
	assert.NotNil(t, nq)
	conns := make([]net.Conn, 0)
	var outerWG sync.WaitGroup

	callback := func(frame []byte, c gnet.Conn) {
		fmt.Println("Callback")
		fmt.Println("message:", frame)
		lenBuf := binary.BigEndian.Uint32(frame[0:4])
		frame = frame[4:]
		if uint32(len(frame)) != lenBuf {
			fmt.Println("length is different")
		}
		request := &pb.RequestProtobuf{}
		err := proto.Unmarshal(frame, request)
		require.NoError(t, err, "Unmarshal Failed")
		msg := message.NewNQMetaMessage(request)
		fmt.Println(msg.Content)

		topicName := "Topic-test-1"
		channelName := "Channel-test-1"
		topic := nq.GetTopic(topicName)
		channel, err := topic.Subscribe(channelName, c)
		require.NoError(t, err)
		go func() {
			fmt.Println("publish message")
			err = channel.Publish(msg)
			require.NoError(t, err, "publish error")
		}()

		go func() {
			fmt.Println("get message")
			info := channel.GetMsg()
			fmt.Println("got message", info)
		}()
	}
	go func() {
		outerWG.Add(1)
		defer func() {
			outerWG.Done()
			fmt.Println("server finished")
		}()
		testServe(t, "tcp", ":6789", false, false, false, 10, gnet.RoundRobin, callback)
	}()
	for i := 0; i < 10; i++ {
		conns = append(conns, newConn())
	}
	for _, conn := range conns {
		outerWG.Add(1)
		go func(c net.Conn) {
			defer func() {
				c.Close()
				outerWG.Done()
			}()
			data, err := message.BuildMessage(&pb.RequestProtobuf{
				Option:  message.OptionPub,
				Topic:   fmt.Sprintf("topic-%d", rand.Intn(100)),
				Channel: fmt.Sprintf("channel-%d", rand.Intn(100)),
				Content: "test content",
			})
			require.NoError(t, err)
			_, err = c.Write(data)
			require.NoError(t, err)
		}(conn)
	}
	outerWG.Wait()

}
