package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/jylc/nijigen-queue/internal/message"
	"github.com/jylc/nijigen-queue/internal/pb"
)

func main() {
	var outerWG sync.WaitGroup
	conns := make([]net.Conn, 0)
	topic := fmt.Sprintf("topic-%d", rand.Intn(100))
	channel := fmt.Sprintf("channel-%d", rand.Intn(100))
	for i := 0; i < 2; i++ {
		conns = append(conns, newConn())
	}
	go func() {
		outerWG.Add(1)
		defer outerWG.Done()
		sub(conns[0], topic, channel)
	}()

	go func() {
		outerWG.Add(1)
		defer outerWG.Done()
		pub(conns[0], 10, topic, channel)
	}()

	outerWG.Wait()
}

func newConn() net.Conn {
	conn, err := net.DialTCP("tcp", nil, &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: 6789,
	})
	if err != nil {
		panic(err)
	}

	err = conn.SetDeadline(time.Time{})
	if err != nil {
		panic(err)
	}
	if err = conn.SetKeepAlive(true); err != nil {
		panic(err)
	}
	if err = conn.SetKeepAlivePeriod(5 * time.Second); err != nil {
		panic(err)
	}
	return conn
}

func sub(conn net.Conn, topic, channel string) {
	ip := conn.LocalAddr().String()

	data, err := message.BuildMessage(&pb.RequestProtobuf{
		Option:  message.OptionSub,
		Topic:   topic,
		Channel: channel,
	})
	if err != nil {
		panic(err)
	}
	request := &pb.RequestProtobuf{}
	if err = proto.Unmarshal(data[4:], request); err != nil {
		logrus.Error(err)
		return
	}
	logrus.Printf("Subscriber[%s] send sub message0:%v", ip, request)

	_, err = conn.Write(data)
	if err != nil {
		panic(err)
	}
	for {
		readBuf := make([]byte, 100)
		var n int
		if n, err = conn.Read(readBuf); err != nil {
			logrus.Error(err)
			return
		}
		readBuf = readBuf[:n]
		if readBuf, err = message.CheckMessage(readBuf); err != nil {
			logrus.Error(err)
			return
		}
		response := &pb.ResponseProtobuf{}
		if err = proto.Unmarshal(readBuf, response); err != nil {
			logrus.Error(err)
			return
		}
		logrus.Printf("Subscriber[%s] recv response from server:%v", ip, request)
		switch response.Option {
		case message.OptionNotify:
			//返回状态，准备接收消息
			logrus.Infof("Subscriber[%s] get notify", ip)
			data, err = message.BuildMessage(&pb.RequestProtobuf{
				Topic:   topic,
				Channel: channel,
				Content: "accept the message",
				Option:  message.OptionReq,
				Timeout: 0,
			})
			if err != nil {
				logrus.Error(err)
			}
			_, err = conn.Write(data)
			if err != nil {
				panic(err)
			}
		case message.OptionAlive:
			logrus.Infof("Subscriber[%s] still alive", ip)
			data, err = message.BuildMessage(&pb.RequestProtobuf{
				Topic:   topic,
				Channel: channel,
				Content: fmt.Sprintf("Subscriber[%s] keep alive", ip),
				Option:  message.OptionAlive,
				Timeout: 0,
			})
			if err != nil {
				logrus.Error(err)
			}
			_, err = conn.Write(data)
			if err != nil {
				panic(err)
			}
		default:
			logrus.Infof("Client[%s] cannot handle response option", ip)
		}
	}
}

func pub(conn net.Conn, times int, topic, channel string) {
	ip := conn.LocalAddr().String()
	for i := 0; i < times; i++ {
		data, err := message.BuildMessage(&pb.RequestProtobuf{
			Option:  message.OptionPub,
			Topic:   topic,
			Channel: channel,
			Content: "test content",
		})
		if err != nil {
			panic(err)
		}
		request := &pb.RequestProtobuf{}
		if err = proto.Unmarshal(data[4:], request); err != nil {
			logrus.Error(err)
			return
		}
		logrus.Printf("Publisher[%s] send pub message0:%v", ip, request)
		_, err = conn.Write(data)
		if err != nil {
			panic(err)
		}

		readBuf := make([]byte, 100)
		var n int
		if n, err = conn.Read(readBuf); err != nil {
			logrus.Error(err)
			return
		}
		readBuf = readBuf[:n]
		if readBuf, err = message.CheckMessage(readBuf); err != nil {
			logrus.Error(err)
			return
		}
		response := &pb.ResponseProtobuf{}
		if err = proto.Unmarshal(readBuf, response); err != nil {
			logrus.Error(err)
			return
		}
		logrus.Printf("Publisher[%s] recv response from server:%v", ip, request)
	}
}
