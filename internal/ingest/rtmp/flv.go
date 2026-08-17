package rtmp

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"

	"github.com/abema/go-mp4"
	"github.com/bluenviron/gortmplib/pkg/message"
)

const (
	TagTypeAudio  uint8 = 8
	TagTypeVideo  uint8 = 9
	TagTypeScript uint8 = 18
)

// FLVHeader is the 13-byte standard FLV header with PreviousTagSize0.
var FLVHeader = []byte{
	'F', 'L', 'V', // Signature
	0x01,                   // Version 1
	0x05,                   // Audio + Video flags
	0x00, 0x00, 0x00, 0x09, // Header length (9 bytes)
	0x00, 0x00, 0x00, 0x00, // PreviousTagSize0
}

// WriteFLVHeader writes the initial FLV stream header to the destination writer.
func WriteFLVHeader(w io.Writer) error {
	_, err := w.Write(FLVHeader)
	return err
}

// WriteFLVTag serializes and writes an FLV tag with the given type, timestamp, and payload.
func WriteFLVTag(w io.Writer, tagType uint8, timestamp uint32, payload []byte) error {
	dataSize := uint32(len(payload))
	header := make([]byte, 11)

	// TagType (1 byte)
	header[0] = tagType

	// DataSize (3 bytes, big endian)
	header[1] = byte(dataSize >> 16)
	header[2] = byte(dataSize >> 8)
	header[3] = byte(dataSize)

	// Timestamp (3 bytes lower 24 bits)
	header[4] = byte(timestamp >> 16)
	header[5] = byte(timestamp >> 8)
	header[6] = byte(timestamp)

	// TimestampExtended (1 byte upper 8 bits)
	header[7] = byte(timestamp >> 24)

	// StreamID (3 bytes, always 0)
	header[8] = 0
	header[9] = 0
	header[10] = 0

	if _, err := w.Write(header); err != nil {
		return err
	}

	if _, err := w.Write(payload); err != nil {
		return err
	}

	// PreviousTagSize (4 bytes big endian = Header 11 bytes + DataSize)
	var prevTagSize [4]byte
	binary.BigEndian.PutUint32(prevTagSize[:], dataSize+11)
	_, err := w.Write(prevTagSize[:])
	return err
}

// marshalMP4 serializes an MP4 box structure into bytes.
func marshalMP4(box mp4.IImmutableBox) ([]byte, error) {
	if box == nil {
		return nil, nil
	}
	var buf bytes.Buffer
	if _, err := mp4.Marshal(&buf, box, mp4.Context{}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// writeFourCC writes a 32-bit FourCC code in big-endian order to dst.
func writeFourCC(dst []byte, fourCC uint32) {
	binary.BigEndian.PutUint32(dst, fourCC)
}

// EncodeVideoToFLV serializes a standard Video message into FLV tag payload and timestamp.
func EncodeVideoToFLV(v *message.Video) ([]byte, uint32, error) {
	var bodyData []byte
	var err error

	switch v.Type {
	case message.VideoTypeConfig:
		switch v.Codec {
		case message.CodecH264:
			bodyData, err = marshalMP4(v.AVCConfig)
		case message.CodecH265:
			bodyData, err = marshalMP4(v.HEVCConfig)
		}
		if err != nil {
			return nil, 0, err
		}
	case message.VideoTypeAU:
		bodyData = v.AU
	}

	body := make([]byte, 5+len(bodyData))
	if v.IsKeyFrame {
		body[0] = 1 << 4
	} else {
		body[0] = 2 << 4
	}
	body[0] |= v.Codec
	body[1] = uint8(v.Type)

	ptsDelta := uint32(v.PTSDelta / time.Millisecond)
	body[2] = uint8(ptsDelta >> 16)
	body[3] = uint8(ptsDelta >> 8)
	body[4] = uint8(ptsDelta)
	copy(body[5:], bodyData)

	return body, uint32(v.DTS / time.Millisecond), nil
}

// EncodeVideoExSequenceStart serializes Enhanced RTMP Video sequence start into FLV tag payload.
func EncodeVideoExSequenceStart(v *message.VideoExSequenceStart) ([]byte, uint32, error) {
	var addBody []byte
	var err error

	switch v.FourCC {
	case message.FourCCAV1:
		addBody, err = marshalMP4(v.AV1Config)
	case message.FourCCVP9:
		addBody, err = marshalMP4(v.VP9Config)
	case message.FourCCHEVC:
		addBody, err = marshalMP4(v.HEVCConfig)
	case message.FourCCAVC:
		addBody, err = marshalMP4(v.AVCConfig)
	}
	if err != nil {
		return nil, 0, err
	}

	body := make([]byte, 5+len(addBody))
	body[0] = 0b10000000 | byte(message.VideoExTypeSequenceStart)
	writeFourCC(body[1:5], v.FourCC)
	copy(body[5:], addBody)

	return body, 0, nil
}

// EncodeVideoExCodedFrames serializes Enhanced RTMP Video frames into FLV tag payload.
func EncodeVideoExCodedFrames(v *message.VideoExCodedFrames) ([]byte, uint32, error) {
	var body []byte

	switch v.FourCC {
	case message.FourCCAVC, message.FourCCHEVC:
		body = make([]byte, 8+len(v.Payload))
		body[0] = 0b10000000 | byte(message.VideoExTypeCodedFrames)
		writeFourCC(body[1:5], v.FourCC)

		tmp := uint32(v.PTSDelta / time.Millisecond)
		body[5] = uint8(tmp >> 16)
		body[6] = uint8(tmp >> 8)
		body[7] = uint8(tmp)
		copy(body[8:], v.Payload)
	default:
		body = make([]byte, 5+len(v.Payload))
		body[0] = 0b10000000 | byte(message.VideoExTypeCodedFrames)
		writeFourCC(body[1:5], v.FourCC)
		copy(body[5:], v.Payload)
	}

	return body, uint32(v.DTS / time.Millisecond), nil
}

// EncodeAudioToFLV serializes a standard Audio message into FLV tag payload and timestamp.
func EncodeAudioToFLV(a *message.Audio) ([]byte, uint32, error) {
	var bodyData []byte

	if a.Codec == message.CodecMPEG4Audio && a.AACType == message.AudioAACTypeConfig {
		if a.AACConfig != nil {
			var err error
			bodyData, err = a.AACConfig.Marshal()
			if err != nil {
				return nil, 0, err
			}
		}
	} else {
		bodyData = a.AU
	}

	var flags byte = a.Codec<<4 | byte(a.Rate)<<2 | byte(a.Depth)<<1
	if a.IsStereo {
		flags |= 1
	}

	var body []byte
	if a.Codec == message.CodecMPEG4Audio {
		body = make([]byte, 2+len(bodyData))
		body[0] = flags
		body[1] = uint8(a.AACType)
		copy(body[2:], bodyData)
	} else {
		body = make([]byte, 1+len(bodyData))
		body[0] = flags
		copy(body[1:], bodyData)
	}

	return body, uint32(a.DTS / time.Millisecond), nil
}

// EncodeAudioExSequenceStart serializes Enhanced RTMP Audio sequence start into FLV tag payload.
func EncodeAudioExSequenceStart(a *message.AudioExSequenceStart) ([]byte, uint32, error) {
	var addBody []byte

	switch a.FourCC {
	case message.FourCCOpus:
		if a.OpusConfig != nil {
			buf, err := a.OpusConfig.Marshal()
			if err != nil {
				return nil, 0, err
			}
			addBody = buf
		}
	case message.FourCCFLAC:
		if a.FlacConfig != nil {
			buf, err := a.FlacConfig.Marshal()
			if err != nil {
				return nil, 0, err
			}
			addBody = buf
		}
	case message.FourCCMP4A:
		if a.AACConfig != nil {
			buf, err := a.AACConfig.Marshal()
			if err != nil {
				return nil, 0, err
			}
			addBody = buf
		}
	}

	body := make([]byte, 5+len(addBody))
	body[0] = (9 << 4) | byte(message.AudioExTypeSequenceStart)
	writeFourCC(body[1:5], a.FourCC)
	copy(body[5:], addBody)

	return body, 0, nil
}

// EncodeAudioExCodedFrames serializes Enhanced RTMP Audio frames into FLV tag payload.
func EncodeAudioExCodedFrames(a *message.AudioExCodedFrames) ([]byte, uint32, error) {
	body := make([]byte, 5+len(a.Payload))
	body[0] = (9 << 4) | byte(message.AudioExTypeCodedFrames)
	writeFourCC(body[1:5], a.FourCC)
	copy(body[5:], a.Payload)

	return body, uint32(a.DTS / time.Millisecond), nil
}

// EncodeDataAMF0ToFLV serializes a gortmplib DataAMF0 message into FLV tag payload.
func EncodeDataAMF0ToFLV(d *message.DataAMF0) ([]byte, uint32, error) {
	body, err := d.Payload.Marshal()
	if err != nil {
		return nil, 0, err
	}
	return body, 0, nil
}
