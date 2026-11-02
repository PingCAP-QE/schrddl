package dump

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/dumpling/export"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
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
		return err
	}
	err = dumper.Dump()
	_ = dumper.Close()
	if err != nil {
		logutil.BgLogger().Warn("dump failed error stack info", zap.Error(err))
		fmt.Printf("\ndump failed: %s\n", err.Error())
		return err
	}

	pt := strings.TrimPrefix(strings.TrimPrefix(fileName, "local://"), "file://")
	return postProcessDump(pt)
}
