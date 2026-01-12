package dump

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"
)

func DumpToFile(dbName string, tblName []string, fileName string, host string) error {
	conf := export.DefaultConfig()
	conf.Host = strings.Split(host, ":")[0]
	conf.Port = 4000

	ns := make([]string, len(tblName))
	for i, v := range tblName {
		ns[i] = fmt.Sprintf("%s.%s", dbName, v)
	}
	var err error
	conf.TableFilter, err = export.ParseTableFilter(ns, []string{"*.*", export.DefaultTableFilter})
	if err != nil {
		logutil.BgLogger().Warn("failed to parse filter", zap.Error(err))
		return err
	}
	conf.OutputDirPath = fileName

	dumper, err := export.NewDumper(context.Background(), conf)
	if err != nil {
		logutil.BgLogger().Warn("create dumper failed", zap.Error(err))
	}
	err = dumper.Dump()
	_ = dumper.Close()
	if err != nil {
		logutil.BgLogger().Warn("dump failed error stack info", zap.Error(err))
		fmt.Printf("\ndump failed: %s\n", err.Error())
		return err
	}

	pt := fileName[8:]
	fs, err := os.ReadDir(pt)
	if err != nil {
		return err
	}
	for _, f := range fs {
		name := f.Name()
		pathFileName := path.Join(pt, name)

		if strings.Contains(name, "schema") {
			if runtime.GOOS == "linux" {
				err = exec.Command("sed", "-i", "1,2d", pathFileName).Run()
			} else {
				err = exec.Command("sed", "-i", "", "1,2d", pathFileName).Run()
			}
			if err != nil {
				logutil.BgLogger().Warn("sed failed", zap.Error(err))
				return err
			}
		} else if strings.Contains(name, "000000") {
			cmd1 := exec.Command("sed", "-i", "", "1,2d", pathFileName)
			if runtime.GOOS == "linux" {
				cmd1 = exec.Command("sed", "-i", "1,2d", pathFileName)
			}
			if err := cmd1.Run(); err != nil {
				return err
			}

			newFileName := strings.Replace(pathFileName, ".000000000", "", 1)

			cmd2 := exec.Command("sh", "-c", fmt.Sprintf("tr -d '\n' < %s > %s", pathFileName, newFileName))
			cmd2.Stdout = os.Stdout
			cmd2.Stderr = os.Stderr
			if err := cmd2.Run(); err != nil {
				return err
			}
			os.Remove(pathFileName)
		}
	}

	return nil
}
