package server

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	currentSourceSegment             = 1
	sourceRecordMagic                = "EWAL"
	sourceRecordVersion         byte = 1
	sourceRecordCodecZstd       byte = 1
	sourceRecordHeaderSize           = 32
	sourceFieldSegment               = "__source_segment"
	sourceFieldOffset                = "__source_offset"
	sourceFieldCompressedLength      = "__source_compressed_length"
	sourceFieldRawLength             = "__source_raw_length"
)

var (
	sourceEncoderPool = sync.Pool{
		New: func() any {
			encoder, err := zstd.NewWriter(nil)
			if err != nil {
				panic(err)
			}
			return encoder
		},
	}
	sourceDecoderPool = sync.Pool{
		New: func() any {
			decoder, err := zstd.NewReader(nil)
			if err != nil {
				panic(err)
			}
			return decoder
		},
	}
)

type sourcePointer struct {
	Segment          int
	Offset           int64
	CompressedLength uint64
	RawLength        uint64
}

type shardSourceReader struct {
	server    *Server
	indexName string
	day       string
	shardID   int
	files     map[int]*os.File
}

func newShardSourceReader(s *Server, indexName, day string, shardID int) *shardSourceReader {
	return &shardSourceReader{
		server:    s,
		indexName: indexName,
		day:       day,
		shardID:   shardID,
		files:     make(map[int]*os.File),
	}
}

func (r *shardSourceReader) Close() error {
	var firstErr error
	for segment, file := range r.files {
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(r.files, segment)
	}
	return firstErr
}

func (r *shardSourceReader) file(segment int) (*os.File, error) {
	if file, ok := r.files[segment]; ok {
		return file, nil
	}

	path := r.server.shardSourceSegmentPath(r.indexName, r.day, r.shardID, segment)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r.files[segment] = file
	return file, nil
}

func (r *shardSourceReader) Read(pointer sourcePointer) (Document, error) {
	if pointer.Segment <= 0 {
		return nil, fmt.Errorf("invalid source segment %d", pointer.Segment)
	}
	file, err := r.file(pointer.Segment)
	if err != nil {
		return nil, err
	}

	raw, err := readSourceRecord(file, pointer)
	if err != nil {
		return nil, err
	}

	var doc Document
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	return doc, nil
}

func (s *Server) shardWriteLock(indexName, day string, shardID int) *sync.Mutex {
	key := partitionKey(indexName, day, shardID)
	if existing, ok := s.shardWriteLocks.Load(key); ok {
		return existing.(*sync.Mutex)
	}

	lock := &sync.Mutex{}
	actual, _ := s.shardWriteLocks.LoadOrStore(key, lock)
	return actual.(*sync.Mutex)
}

func (s *Server) shardSourceSegmentPath(indexName, day string, shardID, segment int) string {
	return filepath.Join(s.shardDayPath(indexName, day), shardSourceSegmentBase(shardID, segment))
}

func shardSourceSegmentBase(shardID, segment int) string {
	return fmt.Sprintf("shard-%02d.wal.%06d", shardID, segment)
}

func shardSourceSegmentPrefix(shardID int) string {
	return fmt.Sprintf("shard-%02d.wal.", shardID)
}

func (s *Server) shardSourceSegmentPaths(indexName, day string, shardID int) ([]string, error) {
	pattern := filepath.Join(s.shardDayPath(indexName, day), shardSourceSegmentPrefix(shardID)+"*")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	return paths, nil
}

func extractedShardSourcePaths(extractedDir string, shardID int) ([]string, error) {
	pattern := filepath.Join(extractedDir, shardSourceSegmentPrefix(shardID)+"*")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	return paths, nil
}

func (s *Server) removeLocalShardFiles(indexName, day string, shardID int) error {
	if err := s.closeCachedShardIndex(indexName, day, shardID); err != nil {
		return err
	}

	if err := os.RemoveAll(s.shardIndexPath(indexName, day, shardID)); err != nil && !os.IsNotExist(err) {
		return err
	}

	paths, err := s.shardSourceSegmentPaths(indexName, day, shardID)
	if err != nil {
		return err
	}
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func compressSourceRecord(raw []byte) []byte {
	encoder := sourceEncoderPool.Get().(*zstd.Encoder)
	defer sourceEncoderPool.Put(encoder)
	return encoder.EncodeAll(raw, nil)
}

func decompressSourceRecord(compressed []byte) ([]byte, error) {
	decoder := sourceDecoderPool.Get().(*zstd.Decoder)
	defer sourceDecoderPool.Put(decoder)
	return decoder.DecodeAll(compressed, nil)
}

func writeSourceRecord(w io.Writer, raw, compressed []byte) error {
	var header [sourceRecordHeaderSize]byte
	copy(header[:4], []byte(sourceRecordMagic))
	header[4] = sourceRecordVersion
	header[5] = sourceRecordCodecZstd
	binary.LittleEndian.PutUint64(header[8:16], uint64(len(raw)))
	binary.LittleEndian.PutUint64(header[16:24], uint64(len(compressed)))
	binary.LittleEndian.PutUint32(header[24:28], crc32.ChecksumIEEE(raw))

	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(compressed)
	return err
}

func readSourceRecord(file *os.File, pointer sourcePointer) ([]byte, error) {
	header := make([]byte, sourceRecordHeaderSize)
	if _, err := file.ReadAt(header, pointer.Offset); err != nil {
		return nil, err
	}

	if string(header[:4]) != sourceRecordMagic {
		return nil, fmt.Errorf("invalid source record magic at offset %d", pointer.Offset)
	}
	if header[4] != sourceRecordVersion {
		return nil, fmt.Errorf("unsupported source record version %d", header[4])
	}
	if header[5] != sourceRecordCodecZstd {
		return nil, fmt.Errorf("unsupported source record codec %d", header[5])
	}

	rawLength := binary.LittleEndian.Uint64(header[8:16])
	compressedLength := binary.LittleEndian.Uint64(header[16:24])
	checksum := binary.LittleEndian.Uint32(header[24:28])

	if pointer.RawLength > 0 && pointer.RawLength != rawLength {
		return nil, fmt.Errorf("source raw length mismatch: got %d want %d", rawLength, pointer.RawLength)
	}
	if pointer.CompressedLength > 0 && pointer.CompressedLength != compressedLength {
		return nil, fmt.Errorf("source compressed length mismatch: got %d want %d", compressedLength, pointer.CompressedLength)
	}

	compressed := make([]byte, compressedLength)
	if _, err := file.ReadAt(compressed, pointer.Offset+sourceRecordHeaderSize); err != nil {
		return nil, err
	}

	raw, err := decompressSourceRecord(compressed)
	if err != nil {
		return nil, err
	}
	if uint64(len(raw)) != rawLength {
		return nil, fmt.Errorf("decoded raw length mismatch: got %d want %d", len(raw), rawLength)
	}
	if got := crc32.ChecksumIEEE(raw); got != checksum {
		return nil, fmt.Errorf("source checksum mismatch: got %08x want %08x", got, checksum)
	}
	return raw, nil
}

func addSourcePointerFields(doc Document, pointer sourcePointer) {
	doc[sourceFieldSegment] = pointer.Segment
	doc[sourceFieldOffset] = pointer.Offset
	doc[sourceFieldCompressedLength] = int64(pointer.CompressedLength)
	doc[sourceFieldRawLength] = int64(pointer.RawLength)
}

func sourcePointerFromFields(fields map[string]interface{}) (sourcePointer, bool, error) {
	segment, okSegment := int64FromValue(fields[sourceFieldSegment])
	offset, okOffset := int64FromValue(fields[sourceFieldOffset])
	compressedLength, okCompressed := int64FromValue(fields[sourceFieldCompressedLength])
	rawLength, okRaw := int64FromValue(fields[sourceFieldRawLength])

	if !okSegment && !okOffset && !okCompressed && !okRaw {
		return sourcePointer{}, false, nil
	}
	if !okSegment || !okOffset || !okCompressed || !okRaw {
		return sourcePointer{}, false, fmt.Errorf("incomplete source pointer fields")
	}
	if segment <= 0 || offset < 0 || compressedLength < 0 || rawLength < 0 {
		return sourcePointer{}, false, fmt.Errorf("invalid source pointer values")
	}

	return sourcePointer{
		Segment:          int(segment),
		Offset:           offset,
		CompressedLength: uint64(compressedLength),
		RawLength:        uint64(rawLength),
	}, true, nil
}

func sourcePointerFieldNames() []string {
	return []string{
		sourceFieldSegment,
		sourceFieldOffset,
		sourceFieldCompressedLength,
		sourceFieldRawLength,
	}
}

func int64FromValue(v interface{}) (int64, bool) {
	switch x := v.(type) {
	case nil:
		return 0, false
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		if x > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(x), true
	case float32:
		return int64(x), true
	case float64:
		return int64(x), true
	case json.Number:
		value, err := x.Int64()
		if err != nil {
			return 0, false
		}
		return value, true
	case string:
		value, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return 0, false
		}
		return value, true
	default:
		return 0, false
	}
}

func materializeBatchItemSource(item internalIndexBatchItem, day string) (string, Document, []byte, error) {
	docID, doc, err := materializeBatchItem(item, day)
	if err != nil {
		return "", nil, nil, err
	}

	raw, err := json.Marshal(doc)
	if err != nil {
		return "", nil, nil, err
	}

	return docID, doc, raw, nil
}

func readSourceDocumentFromFields(reader *shardSourceReader, docID string, fields map[string]interface{}) (Document, error) {
	pointer, ok, err := sourcePointerFromFields(fields)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("missing source pointer for %s", docID)
	}

	doc, err := reader.Read(pointer)
	if err != nil {
		return nil, err
	}
	if _, exists := doc["id"]; !exists {
		doc["id"] = docID
	}
	return doc, nil
}
