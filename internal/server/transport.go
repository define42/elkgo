package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func (s *Server) postJSON(ctx context.Context, url string, body any, out any) error {
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
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
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, buf)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
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
	return postNDJSONWithClient(ctx, &http.Client{Timeout: timeout}, url, docs, out)
}

func postNDJSONWithClient(ctx context.Context, client *http.Client, url string, docs []Document, out any) error {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	for _, doc := range docs {
		if err := enc.Encode(doc); err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
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
	return streamDocumentsWithClient(ctx, &http.Client{Timeout: timeout}, url, onDoc)
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
	return getJSONWithClient(ctx, &http.Client{Timeout: timeout}, url, out)
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
