package server

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

var gzipWriterPool = sync.Pool{
	New: func() any { return gzip.NewWriter(nil) },
}

// gzipReaderPool has no New function because gzip.NewReader requires a
// valid reader to parse the header. First call falls back to gzip.NewReader;
// subsequent calls reuse pooled readers via Reset.
var gzipReaderPool sync.Pool

func (s *Server) postJSON(ctx context.Context, url string, body any, out any) error {
	req, err := newStreamingJSONRequest(ctx, http.MethodPost, url, body, true)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (s *Server) postJSONStatus(ctx context.Context, url string, body any, out any) (int, error) {
	req, err := newStreamingJSONRequest(ctx, http.MethodPost, url, body, true)
	if err != nil {
		return 0, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}
	bodyText := strings.TrimSpace(string(bodyBytes))

	if out != nil && len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, out); err != nil {
			if resp.StatusCode/100 != 2 {
				return resp.StatusCode, fmt.Errorf("status %d: %s", resp.StatusCode, bodyText)
			}
			return resp.StatusCode, err
		}
	}

	if resp.StatusCode/100 != 2 {
		if out != nil && len(bodyBytes) > 0 {
			return resp.StatusCode, nil
		}
		return resp.StatusCode, fmt.Errorf("status %d: %s", resp.StatusCode, bodyText)
	}

	return resp.StatusCode, nil
}

func (s *Server) postNDJSON(ctx context.Context, url string, docs []Document, out any) error {
	return postNDJSONWithClient(ctx, s.client, url, docs, out)
}

func (s *Server) postNDJSONWithTimeout(ctx context.Context, url string, docs []Document, timeout time.Duration, out any) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return postNDJSONWithClient(ctx, s.client, url, docs, out)
}

func postNDJSONWithClient(ctx context.Context, client *http.Client, url string, docs []Document, out any) error {
	req, err := newStreamingNDJSONRequest(ctx, url, docs, true)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (s *Server) getJSON(ctx context.Context, url string, out any) error {
	return getJSONWithClient(ctx, s.client, url, out)
}

func (s *Server) streamDocuments(ctx context.Context, url string, onDoc func(Document) error) error {
	return streamDocumentsWithClient(ctx, s.client, url, onDoc)
}

func (s *Server) streamDocumentsWithTimeout(ctx context.Context, url string, timeout time.Duration, onDoc func(Document) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return streamDocumentsWithClient(ctx, s.client, url, onDoc)
}

func streamDocumentsWithClient(ctx context.Context, client *http.Client, url string, onDoc func(Document) error) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	dec := json.NewDecoder(resp.Body)
	for {
		var doc Document
		if err := dec.Decode(&doc); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := onDoc(doc); err != nil {
			return err
		}
	}
}

func (s *Server) getJSONWithTimeout(ctx context.Context, url string, timeout time.Duration, out any) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return getJSONWithClient(ctx, s.client, url, out)
}

func getJSONWithClient(ctx context.Context, client *http.Client, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func newStreamingJSONRequest(ctx context.Context, method, url string, body any, compress bool) (*http.Request, error) {
	requestBody, headers := newStreamingRequestBody(func(writer io.Writer) error {
		return json.NewEncoder(writer).Encode(body)
	}, "application/json", compress)
	req, err := http.NewRequestWithContext(ctx, method, url, requestBody)
	if err != nil {
		return nil, err
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	return req, nil
}

func newStreamingNDJSONRequest(ctx context.Context, url string, docs []Document, compress bool) (*http.Request, error) {
	requestBody, headers := newStreamingRequestBody(func(writer io.Writer) error {
		enc := json.NewEncoder(writer)
		for _, doc := range docs {
			if err := enc.Encode(doc); err != nil {
				return err
			}
		}
		return nil
	}, "application/x-ndjson", compress)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, requestBody)
	if err != nil {
		return nil, err
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	return req, nil
}

func newStreamingRequestBody(encode func(io.Writer) error, contentType string, compress bool) (io.ReadCloser, map[string]string) {
	pr, pw := io.Pipe()
	headers := map[string]string{
		"Content-Type": contentType,
	}
	if compress {
		headers["Content-Encoding"] = "gzip"
	}

	go func() {
		var err error
		if compress {
			gzipWriter := gzipWriterPool.Get().(*gzip.Writer)
			gzipWriter.Reset(pw)
			err = encode(gzipWriter)
			if closeErr := gzipWriter.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
			gzipWriterPool.Put(gzipWriter)
		} else {
			err = encode(pw)
		}
		_ = pw.CloseWithError(err)
	}()

	return pr, headers
}

func requestBodyReader(r *http.Request) (io.ReadCloser, error) {
	if strings.ToLower(r.Header.Get("Content-Encoding")) != "gzip" {
		return r.Body, nil
	}

	if pooled, ok := gzipReaderPool.Get().(*gzip.Reader); ok {
		if err := pooled.Reset(r.Body); err != nil {
			gzipReaderPool.Put(pooled)
			return nil, err
		}
		return &pooledGzipReadCloser{
			Reader: pooled,
			body:   r.Body,
		}, nil
	}

	reader, err := gzip.NewReader(r.Body)
	if err != nil {
		return nil, err
	}
	return &pooledGzipReadCloser{
		Reader: reader,
		body:   r.Body,
	}, nil
}

type pooledGzipReadCloser struct {
	*gzip.Reader
	body io.ReadCloser
}

func (p *pooledGzipReadCloser) Close() error {
	err := p.Reader.Close()
	gzipReaderPool.Put(p.Reader)
	if bodyErr := p.body.Close(); err == nil {
		err = bodyErr
	}
	return err
}

func decodeJSONRequest(r *http.Request, out any) error {
	reader, err := requestBodyReader(r)
	if err != nil {
		return err
	}
	defer reader.Close()
	return json.NewDecoder(reader).Decode(out)
}

type compositeReadCloser struct {
	io.Reader
	closers []io.Closer
}

func (c *compositeReadCloser) Close() error {
	var firstErr error
	for _, closer := range c.closers {
		if err := closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
