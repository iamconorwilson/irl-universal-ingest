package rtmp

import (
	"bytes"
	"testing"
	"time"

	"github.com/bluenviron/gortmplib/pkg/message"
)

func TestWriteFLVHeader(t *testing.T) {
	var buf bytes.Buffer
	err := WriteFLVHeader(&buf)
	if err != nil {
		t.Fatalf("unexpected error writing FLV header: %v", err)
	}

	if !bytes.Equal(buf.Bytes(), FLVHeader) {
		t.Fatalf("FLV header mismatch: got %v, want %v", buf.Bytes(), FLVHeader)
	}
}

func TestWriteFLVTag(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte{0x01, 0x02, 0x03, 0x04}
	err := WriteFLVTag(&buf, TagTypeVideo, 1000, payload)
	if err != nil {
		t.Fatalf("unexpected error writing FLV tag: %v", err)
	}

	// 11 header + 4 payload + 4 previous tag size = 19 bytes
	if buf.Len() != 19 {
		t.Fatalf("expected 19 bytes, got %d", buf.Len())
	}
}

func TestEncodeVideoToFLV(t *testing.T) {
	msg := &message.Video{
		Codec:      message.CodecH264,
		Type:       message.VideoTypeAU,
		IsKeyFrame: true,
		DTS:        1500 * time.Millisecond,
		PTSDelta:   0,
		AU:         []byte{0x65, 0x88, 0x84},
	}

	payload, ts, err := EncodeVideoToFLV(msg)
	if err != nil {
		t.Fatalf("unexpected error encoding video: %v", err)
	}

	if ts != 1500 {
		t.Errorf("expected timestamp 1500, got %d", ts)
	}

	// Keyframe + H264 = 0x17
	if payload[0] != 0x17 {
		t.Errorf("expected header byte 0x17, got 0x%x", payload[0])
	}
}
