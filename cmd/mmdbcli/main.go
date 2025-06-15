// cmd/mmdbcli/main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/AR1011/slog"
	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/nonhumantrades/mmdb-client/client"
	"github.com/nonhumantrades/mmdb-client/version"
	"github.com/nonhumantrades/mmdb-proto/proto"
	"github.com/urfave/cli/v2"
)

type store struct {
	S3 *proto.S3Config `json:"s3"`
}

/* ───────────── helpers ───────────── */

func cfgPath() string {
	u, _ := user.Current()
	return filepath.Join(u.HomeDir, ".mmdbcli.json")
}

func loadStore() *store {
	data, err := os.ReadFile(cfgPath())
	if err != nil {
		return &store{}
	}
	var s store
	_ = json.Unmarshal(data, &s)
	return &s
}

func saveStore(s *store) {
	b, _ := json.MarshalIndent(s, "", "  ")
	_ = os.WriteFile(cfgPath(), b, 0o600)
}

/* ───────────── S3 config commands ───────────── */

func s3SetAction(c *cli.Context) error {
	cfg := &proto.S3Config{
		Bucket:    c.String("bucket"),
		Url:       c.String("url"),
		AccessKey: c.String("access-key"),
		SecretKey: c.String("secret-key"),
		Region:    c.String("region"),
	}
	for _, v := range []string{cfg.Bucket, cfg.Url, cfg.AccessKey, cfg.SecretKey} {
		if v == "" {
			return fmt.Errorf("all flags are required")
		}
	}
	st := loadStore()
	st.S3 = cfg
	saveStore(st)
	fmt.Println("S3 settings saved.")
	return nil
}

func s3ViewAction(_ *cli.Context) error {
	st := loadStore()
	if st.S3 == nil {
		return fmt.Errorf("S3 not configured")
	}
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleLight)
	t.AppendHeader(table.Row{"SETTING", "VALUE"})
	t.AppendRow(table.Row{"Bucket", st.S3.Bucket})
	t.AppendRow(table.Row{"URL", st.S3.Url})
	t.AppendRow(table.Row{"Access Key", st.S3.AccessKey})
	t.AppendRow(table.Row{"Secret Key", "********"})
	t.AppendRow(table.Row{"Region", st.S3.Region})
	t.Render()
	return nil
}

/* ───────────── backup/restore helpers ───────────── */

func runBackup(
	ctx context.Context,
	cliConn *client.Client,
	req *proto.S3BackupRequest,
) (*proto.S3BackupFooter, error) {

	pw := progress.NewWriter()
	pw.SetAutoStop(true)
	pw.SetTrackerLength(40)
	pw.SetStyle(progress.StyleDefault)
	pw.SetOutputWriter(os.Stdout)
	go pw.Render()

	trk := progress.Tracker{
		Message: "Uploading",
		Units:   progress.UnitsBytes,
		Total:   0, // unknown
	}
	pw.AppendTracker(&trk)

	var footer *proto.S3BackupFooter
	_, err := cliConn.BackupToS3(ctx, (&client.BackupToS3Params{}).
		WithRequest(req).
		WithOnHeader(func(h *proto.S3BackupHeader) error {
			trk.Message = fmt.Sprintf("Uploading %s", h.ObjectKey)
			return nil
		}).
		WithOnProgress(func(p *proto.BytesProgress) error {
			trk.Increment(int64(p.CompletedBytes - uint64(trk.Value())))
			return nil
		}),
	)
	if err != nil {
		return nil, err
	}
	footer = &proto.S3BackupFooter{
		ObjectKey: trk.Message,
		Size:      uint64(trk.Value()),
	}
	pw.Stop()
	return footer, nil
}

func runRestore(ctx context.Context, cliConn *client.Client, req *proto.RestoreFromS3Request) (*proto.S3RestoreFooter, error) {
	pw := progress.NewWriter()
	pw.SetAutoStop(true)
	pw.SetTrackerLength(40)
	pw.SetStyle(progress.StyleBlocks)
	pw.Style().Visibility.ETA = true
	pw.SetOutputWriter(os.Stdout)
	go pw.Render()

	var (
		dl progress.Tracker
		ap progress.Tracker
	)

	var footer *proto.S3RestoreFooter
	_, err := cliConn.RestoreFromS3(ctx, (&client.RestoreFromS3Params{}).
		WithRequest(req).
		WithOnHeader(func(h *proto.S3RestoreHeader) error {
			dl = progress.Tracker{Message: "Download", Units: progress.UnitsBytes}
			ap = progress.Tracker{Message: "Apply", Units: progress.UnitsBytes}
			pw.AppendTracker(&dl)
			pw.AppendTracker(&ap)
			return nil
		}).
		WithOnProgress(func(p *proto.BytesProgress) error {
			switch strings.ToLower(p.Type) {
			case "download":
				dl.Total = int64(p.TotalBytes)
				dl.SetValue(int64(p.CompletedBytes))
			case "apply":
				ap.Total = int64(p.TotalBytes)
				ap.SetValue(int64(p.CompletedBytes))
			}
			return nil
		}),
	)
	if err != nil {
		return nil, err
	}
	pw.Stop()
	footer = &proto.S3RestoreFooter{
		Size:     uint64(ap.Total),
		Duration: uint64(time.Second),
	}
	return footer, nil
}

/* ───────────── command actions ───────────── */

func ensureBackupPrefix(key string) string {
	if !strings.HasPrefix(key, "backup/") {
		return "backup/" + key
	}
	return key
}

func backupAction(incremental bool) cli.ActionFunc {
	return func(c *cli.Context) error {
		st := loadStore()
		if st.S3 == nil {
			return fmt.Errorf("S3 not configured (`mmdbcli s3 set`)")
		}
		if incremental && c.String("full-key") == "" {
			return fmt.Errorf("--full-key required for incremental backup")
		}
		addr := c.String("server")

		ctx := context.Background()
		cliConn, err := client.Dial(ctx, client.Config{Address: addr})
		if err != nil {
			return err
		}
		defer cliConn.Close()

		footer, err := runBackup(ctx, cliConn, &proto.S3BackupRequest{
			S3Config:    st.S3,
			Incremental: incremental,
			FullKey:     ensureBackupPrefix(c.String("full-key")),
		})
		if err != nil {
			return err
		}

		fmt.Printf("\nBackup complete: %s (%s)\n",
			footer.ObjectKey, humanBytes(footer.Size))
		return nil
	}
}

func restoreAction(c *cli.Context) error {
	st := loadStore()
	if st.S3 == nil {
		return fmt.Errorf("S3 not configured")
	}

	addr := c.String("server")
	key := c.String("key")
	if key == "" {
		return fmt.Errorf("--key required")
	}

	ctx := context.Background()
	cliConn, err := client.Dial(ctx, client.Config{Address: addr})
	if err != nil {
		return err
	}
	defer cliConn.Close()

	footer, err := runRestore(ctx, cliConn, &proto.RestoreFromS3Request{
		S3Config:  st.S3,
		ObjectKey: ensureBackupPrefix(key),
	})
	if err != nil {
		return err
	}

	fmt.Printf("\nRestore complete: %s restored (%s)\n",
		key, humanBytes(footer.Size))
	return nil
}

func syncAction(c *cli.Context) error {
	addr := c.String("server")
	tableName := c.String("table")
	if tableName == "" {
		return fmt.Errorf("--table required")
	}

	ctx := context.Background()
	cliConn, err := client.Dial(ctx, client.Config{Address: addr})
	if err != nil {
		return err
	}
	defer cliConn.Close()

	resp, err := cliConn.SyncTable(ctx, tableName)
	if err != nil {
		return err
	}

	fmt.Printf("Table %q synced: %s rows in %s\n",
		tableName, formatNumber(resp.RowCount),
		time.Duration(resp.Duration).Round(time.Millisecond))
	return nil
}

/* ───────────── misc formatting helpers ───────────── */

func humanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func safeDiv(a, b uint64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

func formatNumber(n uint64) string {
	s := fmt.Sprintf("%d", n)
	var out strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out.WriteRune(',')
		}
		out.WriteRune(r)
	}
	return out.String()
}

/* ───────────── list-tables command ───────────── */

func listTablesAction(c *cli.Context) error {
	addr := c.String("server")

	ctx := context.Background()
	cliConn, err := client.Dial(ctx, client.Config{Address: addr})
	if err != nil {
		return err
	}
	defer cliConn.Close()

	tables, err := cliConn.ListTables(ctx)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		fmt.Println("No tables found.")
		return nil
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].CompressedBytes > tables[j].CompressedBytes
	})

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleLight)
	t.AppendHeader(table.Row{"NAME", "ROWS", "COMPRESSED", "UNCOMPRESSED", "RATIO"})
	var totalRows, totalComp, totalUnc uint64
	for _, tbl := range tables {
		ratio := safeDiv(tbl.UncompressedBytes, tbl.CompressedBytes)
		t.AppendRow(table.Row{
			tbl.Name,
			formatNumber(tbl.RowCount),
			humanBytes(tbl.CompressedBytes),
			humanBytes(tbl.UncompressedBytes),
			fmt.Sprintf("%.2fx", ratio),
		})
		totalRows += tbl.RowCount
		totalComp += tbl.CompressedBytes
		totalUnc += tbl.UncompressedBytes
	}
	t.AppendSeparator()
	t.AppendRow(table.Row{
		"TOTAL",
		formatNumber(totalRows),
		humanBytes(totalComp),
		humanBytes(totalUnc),
		fmt.Sprintf("%.2fx", safeDiv(totalUnc, totalComp)),
	})
	t.Render()
	return nil
}

/* ───────────── main ───────────── */

func main() {
	app := &cli.App{
		Name:    "mmdbcli",
		Usage:   "Manage MMDB tables and S3 backups",
		Version: version.Version,
		Commands: []*cli.Command{
			/* S3 config */
			{
				Name:  "s3",
				Usage: "Configure or view S3 credentials",
				Subcommands: []*cli.Command{
					{
						Name:   "set",
						Action: s3SetAction,
						Flags: []cli.Flag{
							&cli.StringFlag{Name: "bucket", Required: true},
							&cli.StringFlag{Name: "url", Required: true},
							&cli.StringFlag{Name: "access-key", Required: true},
							&cli.StringFlag{Name: "secret-key", Required: true},
							&cli.StringFlag{Name: "region", Value: "us-east-1"},
						},
					},
					{
						Name:   "view",
						Action: s3ViewAction,
					},
				},
			},
			/* Backup */
			{
				Name: "backup",
				Subcommands: []*cli.Command{
					{
						Name:   "full",
						Usage:  "Full backup to S3",
						Action: backupAction(false),
						Flags:  []cli.Flag{&cli.StringFlag{Name: "server", Value: "127.0.0.1:7777"}},
					},
					{
						Name:   "inc",
						Usage:  "Incremental backup to S3",
						Action: backupAction(true),
						Flags: []cli.Flag{
							&cli.StringFlag{Name: "server", Value: "127.0.0.1:7777"},
							&cli.StringFlag{Name: "full-key", Required: true},
						},
					},
				},
			},
			/* Restore */
			{
				Name:   "restore",
				Usage:  "Restore database from S3",
				Action: restoreAction,
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "server", Value: "127.0.0.1:7777"},
					&cli.StringFlag{Name: "key", Required: true},
				},
			},
			/* Sync table */
			{
				Name:   "sync",
				Usage:  "Force-sync table metadata",
				Action: syncAction,
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "server", Value: "127.0.0.1:7777"},
					&cli.StringFlag{Name: "table", Required: true},
				},
			},
			/* List tables */
			{
				Name:   "tables",
				Action: listTablesAction,
				Flags:  []cli.Flag{&cli.StringFlag{Name: "server", Value: "127.0.0.1:7777"}},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		slog.Error("mmdbcli", "err", err)
		os.Exit(1)
	}
}
