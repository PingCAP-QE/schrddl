package dump

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// postProcessDump treats file names only as file-system paths, never shell text.
func postProcessDump(directory string) error {
	files, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, file := range files {
		if !file.Type().IsRegular() {
			continue
		}
		name := file.Name()
		isSchema := strings.Contains(name, "schema")
		if !isSchema && !strings.Contains(name, "000000") {
			continue
		}
		source := filepath.Join(directory, name)
		target := source
		if !isSchema {
			target = filepath.Join(directory, strings.Replace(name, ".000000000", "", 1))
		}
		if err := rewriteDumpFile(source, target, !isSchema); err != nil {
			return err
		}
	}
	return nil
}

func rewriteDumpFile(source, target string, removeNewlines bool) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.CreateTemp(filepath.Dir(target), ".dump-*")
	if err != nil {
		return err
	}
	defer os.Remove(output.Name())
	defer output.Close()
	reader := bufio.NewReader(input)
	for skipped := 0; skipped < 2; {
		_, err := reader.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil && err != io.EOF {
			return err
		}
		skipped++
	}
	buffer := make([]byte, 64*1024)
	for {
		n, readErr := reader.Read(buffer)
		data := buffer[:n]
		if removeNewlines {
			data = bytes.ReplaceAll(data, []byte("\n"), nil)
		}
		if _, err := output.Write(data); err != nil {
			return err
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if err := output.Close(); err != nil {
		return err
	}
	if err := os.Rename(output.Name(), target); err != nil {
		return err
	}
	if source != target {
		return os.Remove(source)
	}
	return nil
}
