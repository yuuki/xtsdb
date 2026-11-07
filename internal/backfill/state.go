package backfill

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// State records the sources that were imported completely, one per line,
// so that an interrupted import can be resumed without reading them again.
// Imports are idempotent, so a source that was being imported when the
// import stopped is simply imported again.
type State struct {
	f    *os.File
	done map[string]bool
}

// OpenState opens or creates a state file. With an empty path the state is
// kept in memory only.
func OpenState(path string) (*State, error) {
	s := &State{done: make(map[string]bool)}
	if path == "" {
		return s, nil
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644) //nolint:gosec // G302: lists imported blocks and files, nothing secret
	if err != nil {
		return nil, fmt.Errorf("open state file: %w", err)
	}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			s.done[line] = true
		}
	}
	if err := sc.Err(); err != nil {
		f.Close()
		return nil, fmt.Errorf("read state file: %w", err)
	}
	s.f = f
	return s, nil
}

// Done reports whether a source was imported completely.
func (s *State) Done(id string) bool { return s.done[id] }

// MarkDone records that a source was imported completely.
func (s *State) MarkDone(id string) error {
	s.done[id] = true
	if s.f == nil {
		return nil
	}
	if _, err := fmt.Fprintln(s.f, id); err != nil {
		return fmt.Errorf("write state file: %w", err)
	}
	return s.f.Sync()
}

// Close closes the state file.
func (s *State) Close() error {
	if s.f == nil {
		return nil
	}
	return s.f.Close()
}

// FileID identifies the content of an input file by its absolute path, size
// and modification time.
func FileID(path string) (string, error) {
	st, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("file %s %d %d", abs, st.Size(), st.ModTime().UnixNano()), nil
}
