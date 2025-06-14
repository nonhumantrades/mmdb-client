package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/AR1011/slog"
	"github.com/nonhumantrades/mmdb-client/client"
	"github.com/nonhumantrades/mmdb-client/version"
	"github.com/nonhumantrades/mmdb-proto/proto"
	"github.com/urfave/cli/v2"
)

type store struct {
	S3 *proto.S3Config `json:"s3"`
}

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

func setS3Action(c *cli.Context) error {
	cfg := &proto.S3Config{
		Bucket:    c.String("bucket"),
		Url:       c.String("url"),
		AccessKey: c.String("access-key"),
		SecretKey: c.String("secret-key"),
		Region:    c.String("region"),
	}
	if cfg.Bucket == "" || cfg.Url == "" || cfg.AccessKey == "" || cfg.SecretKey == "" {
		return fmt.Errorf("all flags are required")
	}
	st := loadStore()
	st.S3 = cfg
	saveStore(st)
	fmt.Println("S3 settings saved.")
	return nil
}

func viewS3Action(_ *cli.Context) error {
	st := loadStore()
	out, _ := json.MarshalIndent(st.S3, "", "  ")
	fmt.Println(string(out))
	return nil
}

func backupAction(inc bool) cli.ActionFunc {
	return func(c *cli.Context) error {
		st := loadStore()
		if st.S3 == nil {
			return fmt.Errorf("S3 not configured (`mmdbcli config set`)")
		}
		addr := c.String("server")
		fullKey := c.String("full-key")

		if inc && fullKey == "" {
			return fmt.Errorf("--full-key required for incremental backup")
		}

		ctx := context.Background()
		cliConn, err := client.Dial(ctx, client.Config{Address: addr})
		if err != nil {
			return err
		}
		defer cliConn.Close()

		resp, err := cliConn.BackupToS3(ctx, &proto.S3BackupRequest{
			S3Config:    st.S3,
			Incremental: inc,
			FullKey:     fullKey,
		})
		if err != nil {
			return err
		}
		fmt.Printf("Backup stored: %s (%d bytes) in %v\n",
			resp.ObjectKey, resp.Size, time.Duration(resp.Duration))
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
		return fmt.Errorf("--key is required")
	}

	ctx := context.Background()
	cliConn, err := client.Dial(ctx, client.Config{Address: addr})
	if err != nil {
		return err
	}
	defer cliConn.Close()

	resp, err := cliConn.RestoreFromS3(ctx, &proto.RestoreFromS3Request{
		S3Config:  st.S3,
		ObjectKey: key,
	})
	if err != nil {
		return err
	}
	fmt.Printf("Restore finished in %v, success=%v\n",
		time.Duration(resp.Duration), resp.Success)
	return nil
}

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

func compressionString(c proto.CompressionMethod) string {
	s := proto.CompressionMethod_name[int32(c)]
	return strings.TrimPrefix(s, "Compression")
}

func safeDiv(a, b uint64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

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

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tROWS\tCOMPRESSED\tUNCOMPRESSED\tCOMPRESSION\tRATIO")
	for _, t := range tables {
		ratio := safeDiv(t.UncompressedBytes, t.CompressedBytes)
		fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%s\t%0.2fx\n",
			t.Name,
			t.RowCount,
			humanBytes(t.CompressedBytes),
			humanBytes(t.UncompressedBytes),
			compressionString(t.Compression),
			ratio,
		)
	}
	_ = w.Flush()
	return nil
}

func main() {
	app := &cli.App{
		Name:  "mmdbcli",
		Usage: "Manage MMDB tables and S3 backups",
		Commands: []*cli.Command{
			{
				Name:  "version",
				Usage: "Print the version number",
				Action: func(c *cli.Context) error {
					fmt.Printf("mmdbcli %s\n", version.Version)
					return nil
				},
			},
			{
				Name:  "config",
				Usage: "Configure or view settings",
				Subcommands: []*cli.Command{
					{
						Name:   "set",
						Usage:  "Store S3 credentials",
						Action: setS3Action,
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
						Usage:  "Show stored S3 credentials",
						Action: viewS3Action,
					},
				},
			},
			{
				Name:  "backup",
				Usage: "Create backups",
				Subcommands: []*cli.Command{
					{
						Name:   "full",
						Usage:  "Full backup to S3",
						Action: backupAction(false),
						Flags: []cli.Flag{
							&cli.StringFlag{Name: "server", Value: "127.0.0.1:7777"},
						},
					},
					{
						Name:   "inc",
						Usage:  "Incremental backup to S3",
						Action: backupAction(true),
						Flags: []cli.Flag{
							&cli.StringFlag{Name: "server", Value: "127.0.0.1:7777"},
							&cli.StringFlag{Name: "full-key", Usage: "S3 key of the base full backup", Required: true},
						},
					},
				},
			},
			{
				Name:   "restore",
				Usage:  "Restore database from S3 backup (full or incremental key)",
				Action: restoreAction,
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "server", Value: "127.0.0.1:7777"},
					&cli.StringFlag{Name: "key", Usage: "S3 object key to restore from", Required: true},
				},
			},
			{
				Name:   "tables",
				Usage:  "List tables in the database",
				Action: listTablesAction,
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "server", Value: "127.0.0.1:7777"},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		slog.Error("mmdbcli", "err", err)
		os.Exit(1)
	}
}
