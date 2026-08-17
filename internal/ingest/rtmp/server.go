package rtmp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortmplib/pkg/message"
	"github.com/iamconorwilson/irl-universal-ingest/internal/arbitration"
	"github.com/iamconorwilson/irl-universal-ingest/internal/auth"
	"github.com/iamconorwilson/irl-universal-ingest/internal/relay"
)

// Server implements an RTMP ingest listener wrapping gortmplib.
type Server struct {
	port         int
	allowedPaths []string
	manager      *arbitration.Manager
	relay        *relay.Relay

	listener net.Listener
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewServer creates a new RTMP ingest server instance.
func NewServer(port int, allowedPaths []string, mgr *arbitration.Manager, rel *relay.Relay) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		port:         port,
		allowedPaths: allowedPaths,
		manager:      mgr,
		relay:        rel,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Start begins listening for incoming RTMP connections.
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("starting rtmp listener on %s: %w", addr, err)
	}
	s.listener = l

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// acceptLoop continuously accepts incoming TCP connections.
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				log.Printf("[rtmp] accept error: %v", err)
				continue
			}
		}

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handleConn(c)
		}(conn)
	}
}

// handleConn handles the lifecycle of a single incoming RTMP connection.
func (s *Server) handleConn(nc net.Conn) {
	defer nc.Close()

	sconn := &gortmplib.ServerConn{
		RW: nc,
	}

	if err := sconn.Initialize(); err != nil {
		log.Printf("[rtmp] initialization error from %s: %v", nc.RemoteAddr(), err)
		return
	}

	if err := sconn.Accept(); err != nil {
		log.Printf("[rtmp] handshake error from %s: %v", nc.RemoteAddr(), err)
		return
	}

	streamPath := "/"
	if sconn.URL != nil && sconn.URL.Path != "" {
		streamPath = sconn.URL.Path
	}

	// 1. Verify path authorization
	if !auth.IsAllowed(streamPath, s.allowedPaths) {
		log.Printf("[rtmp] rejected unauthorized path %q from %s", streamPath, nc.RemoteAddr())
		return
	}

	// 2. Claim arbitration slot
	acquired, release := s.manager.TryAcquire("rtmp", streamPath)
	if !acquired {
		log.Printf("[rtmp] rejected connection on path %q from %s: slot already occupied", streamPath, nc.RemoteAddr())
		return
	}
	defer release()

	log.Printf("[rtmp] acquired active ingest slot for path %q from %s", streamPath, nc.RemoteAddr())

	// 3. Start relay session
	writer, err := s.relay.StartSession("", streamPath)
	if err != nil {
		log.Printf("[rtmp] failed to start relay session: %v", err)
		return
	}
	defer s.relay.StopSession()

	if err := WriteFLVHeader(writer); err != nil {
		log.Printf("[rtmp] failed to write FLV header: %v", err)
		return
	}

	// Stream packets from RTMP connection to the relay
	s.streamToRelay(sconn, writer)
	log.Printf("[rtmp] session closed for path %q", streamPath)
}

// streamToRelay reads RTMP messages and writes them as FLV tags into the output writer.
func (s *Server) streamToRelay(sconn *gortmplib.ServerConn, w io.Writer) {
	hasReceivedKeyFrame := false

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		msg, err := sconn.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Printf("[rtmp] read error: %v", err)
			}
			return
		}

		s.manager.Touch()

		switch m := msg.(type) {
		case *message.Video:
			payload, ts, err := EncodeVideoToFLV(m)
			if err == nil {
				s.manager.AddBytes(uint64(len(payload)))

				if m.Type == message.VideoTypeAU {
					if !hasReceivedKeyFrame {
						if !m.IsKeyFrame {
							continue
						}
						hasReceivedKeyFrame = true
						log.Printf("[rtmp] first video keyframe received, streaming output")
					}
				}

				if err := WriteFLVTag(w, TagTypeVideo, ts, payload); err != nil {
					log.Printf("[rtmp] write video tag error: %v", err)
					return
				}
			}

		case *message.VideoExSequenceStart:
			payload, ts, err := EncodeVideoExSequenceStart(m)
			if err == nil {
				s.manager.AddBytes(uint64(len(payload)))
				if err := WriteFLVTag(w, TagTypeVideo, ts, payload); err != nil {
					log.Printf("[rtmp] write video ex sequence tag error: %v", err)
					return
				}
			}

		case *message.VideoExCodedFrames:
			hasReceivedKeyFrame = true
			payload, ts, err := EncodeVideoExCodedFrames(m)
			if err == nil {
				s.manager.AddBytes(uint64(len(payload)))
				if err := WriteFLVTag(w, TagTypeVideo, ts, payload); err != nil {
					log.Printf("[rtmp] write video ex frames tag error: %v", err)
					return
				}
			}

		case *message.Audio:
			payload, ts, err := EncodeAudioToFLV(m)
			if err == nil {
				s.manager.AddBytes(uint64(len(payload)))

				if !hasReceivedKeyFrame && m.AACType != message.AudioAACTypeConfig {
					continue
				}

				if err := WriteFLVTag(w, TagTypeAudio, ts, payload); err != nil {
					log.Printf("[rtmp] write audio tag error: %v", err)
					return
				}
			}

		case *message.AudioExSequenceStart:
			payload, ts, err := EncodeAudioExSequenceStart(m)
			if err == nil {
				s.manager.AddBytes(uint64(len(payload)))
				if err := WriteFLVTag(w, TagTypeAudio, ts, payload); err != nil {
					log.Printf("[rtmp] write audio ex sequence tag error: %v", err)
					return
				}
			}

		case *message.AudioExCodedFrames:
			payload, ts, err := EncodeAudioExCodedFrames(m)
			if err == nil {
				s.manager.AddBytes(uint64(len(payload)))
				if !hasReceivedKeyFrame {
					continue
				}
				if err := WriteFLVTag(w, TagTypeAudio, ts, payload); err != nil {
					log.Printf("[rtmp] write audio ex frames tag error: %v", err)
					return
				}
			}

		case *message.DataAMF0:
			payload, ts, err := EncodeDataAMF0ToFLV(m)
			if err == nil {
				s.manager.AddBytes(uint64(len(payload)))
				if err := WriteFLVTag(w, TagTypeScript, ts, payload); err != nil {
					log.Printf("[rtmp] write metadata tag error: %v", err)
					return
				}
			}
		}
	}
}

// Close gracefully stops the RTMP listener and waits for active connections to finish.
func (s *Server) Close() error {
	s.cancel()
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
	return nil
}
