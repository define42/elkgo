package server

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/index/scorch"
	bleveindex "github.com/blevesearch/bleve_index_api"
)

type filesystemDirectoryWriter struct {
	baseDir string
}

func (w *filesystemDirectoryWriter) GetWriter(filePath string) (io.WriteCloser, error) {
	name := filepath.Clean(strings.TrimSpace(filePath))
	name = strings.TrimPrefix(name, string(os.PathSeparator))
	if name == "." || name == "" {
		return nil, fmt.Errorf("snapshot file path is empty")
	}
	targetPath := filepath.Join(w.baseDir, name)
	if !strings.HasPrefix(targetPath, w.baseDir+string(os.PathSeparator)) && targetPath != w.baseDir {
		return nil, fmt.Errorf("invalid snapshot file path %q", filePath)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return nil, err
	}
	return os.Create(targetPath)
}

func materializeShardSnapshot(idx bleve.Index, destDir string) error {
	advanced, err := idx.Advanced()
	if err != nil {
		return err
	}
	reader, err := advanced.Reader()
	if err != nil {
		return err
	}
	snapshot, ok := reader.(*scorch.IndexSnapshot)
	if !ok {
		_ = reader.Close()
		return fmt.Errorf("snapshot copy is not supported for index type %T", reader)
	}
	defer snapshot.Close()

	dirWriter := &filesystemDirectoryWriter{baseDir: destDir}
	if err := copyIndexMetadataToDirectory(idx.Name(), dirWriter); err != nil {
		return err
	}
	if err := snapshot.CopyTo(dirWriter); err != nil {
		return err
	}
	_ = snapshot.CloseCopyReader()
	return nil
}

func (s *Server) writeShardSnapshotArchive(indexName, day string, shardID int, idx bleve.Index, archivePath string) error {
	tempDir, err := os.MkdirTemp(filepath.Dir(archivePath), "shard-snapshot-materialized-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	tempIndexPath := filepath.Join(tempDir, filepath.Base(s.shardIndexPath(indexName, day, shardID)))
	if err := materializeShardSnapshot(idx, tempIndexPath); err != nil {
		return err
	}
	if err := s.copyShardSourceSegments(indexName, day, shardID, tempDir); err != nil {
		return err
	}
	return zipDirectory(tempDir, archivePath)
}

func copyIndexMetadataToDirectory(indexPath string, dir bleveindex.Directory) error {
	metaPath := filepath.Join(indexPath, "index_meta.json")
	metaBytes, err := os.ReadFile(metaPath)
	if err != nil {
		return err
	}
	writer, err := dir.GetWriter("index_meta.json")
	if err != nil {
		return err
	}
	defer writer.Close()
	_, err = writer.Write(metaBytes)
	return err
}

func copyFile(srcPath, dstPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}

	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(dst, src)
	closeErr := dst.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func (s *Server) copyShardSourceSegments(indexName, day string, shardID int, destDir string) error {
	paths, err := s.shardSourceSegmentPaths(indexName, day, shardID)
	if err != nil {
		return err
	}
	for _, path := range paths {
		if err := copyFile(path, filepath.Join(destDir, filepath.Base(path))); err != nil {
			return err
		}
	}
	return nil
}

func zipDirectory(srcDir, archivePath string) error {
	archiveFile, err := os.Create(archivePath)
	if err != nil {
		return err
	}

	zipWriter := zip.NewWriter(archiveFile)

	walkErr := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info == nil || info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		relPath = filepath.ToSlash(relPath)

		entryWriter, err := zipWriter.Create(relPath)
		if err != nil {
			return err
		}
		srcFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer srcFile.Close()

		_, err = io.Copy(entryWriter, srcFile)
		return err
	})
	if walkErr != nil {
		_ = zipWriter.Close()
		_ = archiveFile.Close()
		return walkErr
	}
	if err := zipWriter.Close(); err != nil {
		_ = archiveFile.Close()
		return err
	}
	return archiveFile.Close()
}

func (s *Server) restoreShardSnapshotFromCandidates(ctx context.Context, task shardSyncTask) (bool, string, error) {
	candidates := sourceReplicaCandidates(task.previous, task.current, s.nodeID)
	errorsOut := make([]string, 0, len(candidates))

	requestURLSuffix := fmt.Sprintf(
		"/internal/snapshot_shard?index=%s&day=%s&shard=%d",
		url.QueryEscape(task.current.IndexName),
		url.QueryEscape(task.current.Day),
		task.current.ShardID,
	)

	for _, nodeID := range candidates {
		addr, ok := s.memberAddr(nodeID)
		if !ok {
			errorsOut = append(errorsOut, nodeID+": not registered")
			continue
		}

		restored, err := s.restoreShardSnapshotFromURL(ctx, task.current, addr+requestURLSuffix, shardSyncTimeout)
		if err != nil {
			errorsOut = append(errorsOut, nodeID+": "+err.Error())
			continue
		}
		if restored {
			return true, nodeID, nil
		}
		return false, "", nil
	}

	if len(errorsOut) == 0 {
		return false, "", fmt.Errorf("no source replicas available")
	}
	return false, "", fmt.Errorf("%s", strings.Join(errorsOut, "; "))
}

func (s *Server) restoreShardSnapshotFromURL(ctx context.Context, route RoutingEntry, snapshotURL string, timeout time.Duration) (bool, error) {
	if s.localShardExists(route.IndexName, route.Day, route.ShardID) {
		return false, nil
	}

	livePath := s.shardIndexPath(route.IndexName, route.Day, route.ShardID)
	parentDir := filepath.Dir(livePath)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		return false, err
	}

	tempDir, err := os.MkdirTemp(parentDir, fmt.Sprintf("shard-%02d-snapshot-", route.ShardID))
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(tempDir)

	tempArchivePath := filepath.Join(tempDir, "snapshot.zip")
	if err := downloadSnapshotArchive(ctx, s.client, snapshotURL, timeout, tempArchivePath); err != nil {
		return false, err
	}

	if err := extractSnapshotArchive(tempArchivePath, tempDir); err != nil {
		return false, err
	}

	if s.localShardExists(route.IndexName, route.Day, route.ShardID) {
		return false, nil
	}
	if err := s.installExtractedShardSnapshot(route.IndexName, route.Day, route.ShardID, tempDir, false); err != nil {
		if s.localShardExists(route.IndexName, route.Day, route.ShardID) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Server) installShardSnapshot(indexName, day string, shardID int, archive io.Reader, replaceExisting bool) error {
	livePath := s.shardIndexPath(indexName, day, shardID)
	parentDir := filepath.Dir(livePath)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		return err
	}

	tempDir, err := os.MkdirTemp(parentDir, fmt.Sprintf("shard-%02d-install-", shardID))
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	tempArchivePath := filepath.Join(tempDir, "snapshot.zip")
	tempArchive, err := os.Create(tempArchivePath)
	if err != nil {
		return err
	}
	if _, err := io.Copy(tempArchive, archive); err != nil {
		_ = tempArchive.Close()
		return err
	}
	if err := tempArchive.Close(); err != nil {
		return err
	}

	if err := extractSnapshotArchive(tempArchivePath, tempDir); err != nil {
		return err
	}

	return s.installExtractedShardSnapshot(indexName, day, shardID, tempDir, replaceExisting)
}

func (s *Server) installExtractedShardSnapshot(indexName, day string, shardID int, extractedDir string, replaceExisting bool) error {
	livePath := s.shardIndexPath(indexName, day, shardID)
	parentDir := filepath.Dir(livePath)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		return err
	}

	lock := s.shardWriteLock(indexName, day, shardID)
	lock.Lock()
	defer lock.Unlock()

	if replaceExisting {
		if err := s.removeLocalShardFiles(indexName, day, shardID); err != nil {
			return err
		}
	} else if s.localShardExists(indexName, day, shardID) {
		return nil
	}

	extractedWalPaths, err := extractedShardSourcePaths(extractedDir, shardID)
	if err != nil {
		return err
	}
	for _, walPath := range extractedWalPaths {
		if err := os.Rename(walPath, filepath.Join(parentDir, filepath.Base(walPath))); err != nil {
			return err
		}
	}

	extractedIndexPath := filepath.Join(extractedDir, filepath.Base(livePath))
	if _, err := os.Stat(extractedIndexPath); err != nil {
		return err
	}
	return os.Rename(extractedIndexPath, livePath)
}

func (s *Server) transferShardSnapshotToReplica(ctx context.Context, route RoutingEntry, nodeID string) error {
	addr, ok := s.memberAddr(nodeID)
	if !ok {
		return fmt.Errorf("replica not registered")
	}

	idx, err := s.openExistingShardIndex(route.IndexName, route.Day, route.ShardID)
	if err != nil {
		return err
	}

	archiveFile, err := os.CreateTemp("", fmt.Sprintf("elkgo-repair-shard-%02d-*.zip", route.ShardID))
	if err != nil {
		return err
	}
	archivePath := archiveFile.Name()
	if err := archiveFile.Close(); err != nil {
		return err
	}
	defer os.Remove(archivePath)

	if err := s.writeShardSnapshotArchive(route.IndexName, route.Day, route.ShardID, idx, archivePath); err != nil {
		return err
	}

	archiveReader, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer archiveReader.Close()

	requestURL := fmt.Sprintf(
		"%s/internal/install_snapshot_shard?index=%s&day=%s&shard=%d",
		addr,
		url.QueryEscape(route.IndexName),
		url.QueryEscape(route.Day),
		route.ShardID,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, archiveReader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/zip")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func downloadSnapshotArchive(ctx context.Context, client *http.Client, snapshotURL string, timeout time.Duration, destPath string) error {
	requestClient := client
	if timeout > 0 {
		requestClient = &http.Client{Timeout: timeout}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, snapshotURL, nil)
	if err != nil {
		return err
	}
	resp, err := requestClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	file, err := os.Create(destPath)
	if err != nil {
		return err
	}
	if _, err := io.Copy(file, resp.Body); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func extractSnapshotArchive(archivePath, destDir string) error {
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return err
	}

	for _, file := range reader.File {
		cleanName := filepath.Clean(file.Name)
		if cleanName == "." || cleanName == "" {
			continue
		}
		targetPath := filepath.Join(destDir, cleanName)
		if !strings.HasPrefix(targetPath, destDir+string(os.PathSeparator)) && targetPath != destDir {
			return fmt.Errorf("invalid snapshot path %q", file.Name)
		}

		if file.FileInfo().IsDir() {
			if err := os.MkdirAll(targetPath, 0o755); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
			return err
		}
		src, err := file.Open()
		if err != nil {
			return err
		}
		dst, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			src.Close()
			return err
		}
		_, copyErr := io.Copy(dst, src)
		closeErr := dst.Close()
		srcErr := src.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if srcErr != nil {
			return srcErr
		}
	}

	return nil
}

var _ bleveindex.Directory = (*filesystemDirectoryWriter)(nil)
