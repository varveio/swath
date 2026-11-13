// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command swath-replay serves a sorted listing fixture through the S3
// ListObjectsV2 route, for client tests and benchmarks without a bucket.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/varveio/swath/internal/buildinfo"
	"github.com/varveio/swath/internal/replay/javanum"
	"github.com/varveio/swath/internal/replay/latency"
	"github.com/varveio/swath/internal/replay/server"
	"github.com/varveio/swath/internal/replay/sortfixture"
	"github.com/varveio/swath/internal/replay/store"
	"github.com/varveio/swath/internal/replay/store/memstore"
)

// usageError is a bad command line: exit 2.
type usageError struct{ error }

// ownMessage is a failure whose message already names the command
// ("sort-fixture: ..."), printed as it is: exit 1.
type ownMessage struct{ error }

func main() {
	err := run(os.Args[1:], os.Stdout, os.Stderr)
	var sig exitSignal
	switch {
	case err == nil:
	case errors.As(err, &sig):
		os.Exit(int(sig))
	case errors.As(err, new(usageError)):
		fmt.Fprintln(os.Stderr, "swath-replay:", err)
		os.Exit(2)
	case errors.As(err, new(ownMessage)):
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	default:
		fmt.Fprintln(os.Stderr, "swath-replay:", err)
		os.Exit(1)
	}
}

// exitSignal ends a server stopped by a signal: 128 + the signal number.
type exitSignal int

func (e exitSignal) Error() string { return "stopped by signal" }

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		usage(stderr)
		return usageError{errors.New("a command is required")}
	}
	switch args[0] {
	case "serve":
		return serve(args[1:], stderr)
	case "sort-fixture":
		return sortFixture(args[1:], stdout, stderr)
	case "version", "-version", "--version", "-V":
		fmt.Fprintln(stdout, version())
		return nil
	case "help", "-h", "-help", "--help":
		usage(stdout)
		return nil
	}
	if strings.HasPrefix(args[0], "-") {
		// The serve flags are accepted without the command, as the Java
		// launcher accepts them.
		return serve(args, stderr)
	}
	usage(stderr)
	return usageError{fmt.Errorf("unknown command %q", args[0])}
}

func usage(w io.Writer) {
	fmt.Fprint(w, `usage:
  swath-replay serve --fixture PATH --bucket NAME [flags]   serve a fixture through the S3 route
  swath-replay sort-fixture --capture PATH --output DIR [--key-limit N]
                                                            sort a capture into a fixture
  swath-replay --version                                     print the build
Run "swath-replay serve -h" for the serve flags.
`)
}

// version is this binary's build (internal/buildinfo: the version and
// revision go/build.sh links in) under this command's name.
func version() string {
	b := buildinfo.Read()
	if b.Revision == "" {
		return "swath-replay " + b.Version
	}
	return "swath-replay " + b.Version + " (" + b.Revision + ")"
}

// sortFixture sorts an unsorted capture into a SeekParquet fixture. The
// summary line goes to stdout and a refusal to stderr as one line, exit 1,
// the split a launcher script reading the summary relies on.
func sortFixture(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("sort-fixture", flag.ContinueOnError)
	fs.SetOutput(stderr)
	capture := fs.String("capture", "", "the unsorted capture: one part file or a directory of *.parquet parts")
	output := fs.String("output", "", "the new or empty directory the sorted dataset is published into")
	keyLimit := fs.Int("key-limit", 0, fmt.Sprintf("the key cap in bytes, at most %d (0: %d)", sortfixture.MaxKeyLimit, sortfixture.DefaultKeyLimit))
	if err := fs.Parse(args); err != nil {
		return usageError{err}
	}
	switch {
	case fs.NArg() > 0:
		return usageError{fmt.Errorf("sort-fixture: unexpected argument %q", fs.Arg(0))}
	case *capture == "" || *output == "":
		return usageError{errors.New("sort-fixture: --capture and --output are required")}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if _, err := sortfixture.Run(ctx, sortfixture.Options{Capture: *capture, Output: *output, KeyLimit: *keyLimit}, stdout); err != nil {
		return ownMessage{err}
	}
	return nil
}

// serveFlags are the serve command's flags, with the defaults of the
// replay wire contract's launcher.
type serveFlags struct {
	fixture, bucket, host, protocols, azureAccount, advertisedHost string
	servingMode, injectLatency                                     string
	stopTimeout, idleTimeout, writeTimeout                         string
	port, metricsPort, parquetConnections, maxConcurrentRequests   int
	prefetchWindowRows, prefetchMaxWindows                         int
	maxResponseBytes                                               int
	responseBufferBudget, slowRequestLogMs, listIndexMaxBytes      int64
	latencyJitter, latencyScale                                    float64
	azureHNS, listIndex                                            bool
}

// slowLogEnv sets the slow-request log threshold when --slow-request-log-ms
// is not given.
const slowLogEnv = "SWATH_REPLAY_SLOW_REQUEST_LOG_MS"

func parseServe(args []string, stderr io.Writer) (*serveFlags, error) {
	f := &serveFlags{}
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&f.fixture, "fixture", "", "the sorted fixture: a dataset's data directory, one sorted part, or a .tsv key list")
	fs.StringVar(&f.bucket, "bucket", "", "the bucket name to expose")
	fs.StringVar(&f.host, "host", "127.0.0.1", "bind host")
	fs.IntVar(&f.port, "port", 0, "bind port; 0 chooses a free port, reported in the startup line")
	fs.StringVar(&f.protocols, "protocols", "s3", "comma-separated listing routes: s3, gcs, azure")
	fs.StringVar(&f.azureAccount, "azure-account", "", "Azure account path segment (default replay; needs azure in --protocols)")
	fs.BoolVar(&f.azureHNS, "azure-hns", false, "answer Azure Get Account Information as a hierarchical-namespace account (needs azure in --protocols)")
	fs.StringVar(&f.advertisedHost, "advertised-host", "", "host to advertise in native response URLs (the Azure ServiceEndpoint)")
	fs.StringVar(&f.servingMode, "serving-mode", "sorted", "how to serve the fixture: sorted")
	fs.IntVar(&f.parquetConnections, "parquet-connections", 0, "concurrent fixture page decodes; 0 is max(8, min(32, GOMAXPROCS))")
	fs.IntVar(&f.prefetchWindowRows, "prefetch-window-rows", store.DefaultPrefetchWindowRows, "rows a walk's window cache fill ramps to; 0 turns the window cache off")
	fs.IntVar(&f.prefetchMaxWindows, "prefetch-max-windows", store.DefaultPrefetchMaxWindows, "windows the window cache holds (at most this many times --prefetch-window-rows rows); 0 turns it off")
	fs.BoolVar(&f.listIndex, "list-index", true, "build the '/' listing index of a SeekParquet dataset at startup, so a delimited listing skips a wide common prefix without decoding its key pages")
	fs.Int64Var(&f.listIndexMaxBytes, "list-index-max-bytes", store.DefaultListIndexMaxBytes, "memory the listing index retains at most; row groups past it are listed without it")
	fs.IntVar(&f.maxConcurrentRequests, "max-concurrent-requests", server.DefaultMaxConcurrentRequests,
		"requests served at once; more queue. Injected delay holds a slot, so set it above the widest client fan-out")
	fs.Int64Var(&f.responseBufferBudget, "response-buffer-budget", server.DefaultResponseBufferBudget, "bytes of encoded responses held at once")
	fs.IntVar(&f.maxResponseBytes, "max-response-bytes", server.DefaultMaxResponseBytes, "largest encoded response")
	fs.StringVar(&f.stopTimeout, "stop-timeout", "10s", "shutdown deadline")
	fs.StringVar(&f.idleTimeout, "idle-timeout", "30s", "connection idle timeout")
	fs.StringVar(&f.writeTimeout, "write-timeout", "30s", "response write timeout, from the first byte written")
	fs.IntVar(&f.metricsPort, "metrics-port", -1, "serve /metrics, /healthz and /runtime-attestation on this port; negative disables, 0 picks one")
	fs.StringVar(&f.injectLatency, "inject-latency", "", "per-shape target latency: prod-commoncrawl, or e.g. worker_page=223ms,pivot_probe=121ms,structure_probe=223ms+55ms/cp")
	fs.Float64Var(&f.latencyJitter, "latency-jitter", 0, "deterministic jitter fraction in [0,1), keyed by the request")
	fs.Float64Var(&f.latencyScale, "latency-scale", 1, "divide every injected delay by this factor")
	slowDefault := int64(-1)
	if v := os.Getenv(slowLogEnv); v != "" {
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return nil, usageError{fmt.Errorf("%s: %w", slowLogEnv, err)}
		}
		slowDefault = n
	}
	fs.Int64Var(&f.slowRequestLogMs, "slow-request-log-ms", slowDefault,
		"log requests whose own cost reaches this many milliseconds; negative disables (default from "+slowLogEnv+")")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, err
		}
		return nil, usageError{err}
	}
	if fs.NArg() > 0 {
		return nil, usageError{fmt.Errorf("unexpected argument %q", fs.Arg(0))}
	}
	if f.fixture == "" || f.bucket == "" {
		fs.Usage()
		return nil, usageError{errors.New("--fixture and --bucket are required")}
	}
	return f, nil
}

func serve(args []string, stderr io.Writer) error {
	f, err := parseServe(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	cfg, err := f.config()
	if err != nil {
		return usageError{err}
	}
	cfg.Log = stderr
	profile, err := latency.Parse(f.injectLatency, f.latencyJitter, f.latencyScale)
	if err != nil {
		return usageError{err}
	}
	cfg.Latency = profile

	readers := f.parquetConnections
	if readers <= 0 {
		readers = store.DefaultReaders()
	}
	opened := time.Now()
	st, mode, meta, err := openFixture(f.fixture, store.Options{Readers: readers, Prefetch: f.prefetch(),
		ListIndex: store.ListIndex{Off: !f.listIndex, MaxBytes: f.listIndexMaxBytes}})
	if err != nil {
		return err
	}
	cfg.Store, cfg.ServingMode, cfg.FixtureIdentity = st, mode, meta.Identity
	if mode == "SORTED" {
		// The routing index's load, without the listing index's build,
		// which has its own meter.
		cfg.IndexLoad, cfg.IndexEntries = time.Since(opened)-meta.ListIndex.Stats.Duration, meta.RowGroups
	}
	srv, err := startServer(cfg, stderr)
	if err != nil {
		return err
	}
	stopProfile, err := devProfile()
	if err != nil {
		srv.Shutdown(cfg.StopTimeout)
		return err
	}
	defer stopProfile()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()
	fmt.Fprintln(stderr, startupLine(f, srv, cfg, readers))
	fmt.Fprintln(stderr, listIndexLine(meta.ListIndex))
	select {
	case err := <-served:
		if _, cerr := srv.Shutdown(cfg.StopTimeout); err == nil {
			err = cerr
		}
		return err
	case sig := <-sigs:
		// A store close failure is already in the log; the hook line
		// follows either way, as the launcher's shutdown hook prints it.
		srv.Shutdown(cfg.StopTimeout)
		fmt.Fprintln(stderr, "swath_replay_shutdown_hook complete")
		if s, ok := sig.(syscall.Signal); ok {
			return exitSignal(128 + int(s))
		}
		return exitSignal(130)
	}
}

// devProfile starts the CPU profile and arranges the allocation profile named
// by the environment (SWATH_CPUPROFILE, SWATH_MEMPROFILE, the lister's
// names), for development: both are written when the server stops.
func devProfile() (stop func(), err error) {
	var stops []func()
	if path := os.Getenv("SWATH_CPUPROFILE"); path != "" {
		f, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			return nil, err
		}
		stops = append(stops, func() { pprof.StopCPUProfile(); f.Close() })
	}
	if path := os.Getenv("SWATH_MEMPROFILE"); path != "" {
		runtime.MemProfileRate = 64 << 10
		stops = append(stops, func() {
			if f, err := os.Create(path); err == nil {
				_ = pprof.Lookup("allocs").WriteTo(f, 0)
				f.Close()
			}
		})
	}
	return func() {
		for _, s := range stops {
			s()
		}
	}, nil
}

// startServer builds and binds the server over cfg.Store, which it owns
// from here: when either step fails the store is closed, the original
// error is returned as it was (a bad configuration still a usage error),
// and a failure to close the store is reported once on stderr rather than
// replacing it.
func startServer(cfg server.Config, stderr io.Writer) (*server.Server, error) {
	srv, err := server.New(cfg)
	if err == nil {
		if err = srv.Listen(); err == nil {
			return srv, nil
		}
	} else {
		err = usageError{err}
	}
	if cerr := cfg.Store.Close(); cerr != nil {
		fmt.Fprintf(stderr, "replay store cleanup failed after a failed start: %v\n", cerr)
	}
	return nil, err
}

// openFixture opens a .tsv key list in memory, anything else as a sorted
// fixture.
// prefetch is the window cache the flags configure.
func (f *serveFlags) prefetch() store.Prefetch {
	return store.Prefetch{
		Off:        f.prefetchWindowRows == 0 || f.prefetchMaxWindows == 0,
		WindowRows: f.prefetchWindowRows, MaxWindows: f.prefetchMaxWindows,
	}
}

func openFixture(path string, opts store.Options) (store.Store, string, store.Metadata, error) {
	if strings.HasSuffix(path, ".tsv") {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, "", store.Metadata{}, err
		}
		objs, err := memstore.ReadTSV(strings.NewReader(string(b)))
		if err != nil {
			return nil, "", store.Metadata{}, err
		}
		st, err := memstore.New(objs, memstore.Options{Rollup: true})
		if err != nil {
			return nil, "", store.Metadata{}, err
		}
		sum := sha256.Sum256(b)
		return st, "MEMORY", store.Metadata{Identity: hex.EncodeToString(sum[:])}, nil
	}
	st, meta, err := store.Open(context.Background(), path, opts)
	if err != nil {
		return nil, "", store.Metadata{}, fmt.Errorf("open fixture %s: %w", path, err)
	}
	return st, "SORTED", meta, nil
}

func (f *serveFlags) config() (server.Config, error) {
	cfg := server.Config{
		Bucket: f.bucket, Host: f.host, Port: f.port, MetricsPort: f.metricsPort,
		MaxConcurrentRequests: f.maxConcurrentRequests, ResponseBufferBudget: f.responseBufferBudget,
		MaxResponseBytes: f.maxResponseBytes, SlowRequestLog: time.Duration(f.slowRequestLogMs) * time.Millisecond,
	}
	if f.slowRequestLogMs < 0 {
		cfg.SlowRequestLog = -1
	}
	if f.prefetchWindowRows < 0 || f.prefetchMaxWindows < 0 {
		return cfg, errors.New("--prefetch-window-rows and --prefetch-max-windows must not be negative (0 turns the window cache off)")
	}
	if f.listIndexMaxBytes <= 0 {
		return cfg, fmt.Errorf("--list-index-max-bytes must be positive, got %d", f.listIndexMaxBytes)
	}
	switch strings.ToLower(f.servingMode) {
	case "sorted":
	case "duckdb":
		return cfg, errors.New("--serving-mode duckdb is not available: this server serves sorted fixtures only (sort a capture with sort-fixture)")
	default:
		return cfg, fmt.Errorf("--serving-mode must be sorted, got %q", f.servingMode)
	}
	protocols, err := parseProtocols(f.protocols)
	if err != nil {
		return cfg, err
	}
	if err := server.ValidateProtocols(protocols, f.bucket); err != nil {
		return cfg, err
	}
	cfg.Protocols, cfg.AzureAccount, cfg.AdvertisedHost, cfg.AzureHNS = protocols, f.azureAccount, f.advertisedHost, f.azureHNS
	if f.azureAccount == "" && slices.Contains(protocols, server.ProtocolAzure) {
		cfg.AzureAccount = server.DefaultAzureAccount
	}
	if f.advertisedHost != "" && !validAdvertisedHost(f.advertisedHost) {
		return cfg, errors.New("--advertised-host must be a concrete host without scheme or port")
	}
	for _, d := range []struct {
		name string
		v    string
		dst  *time.Duration
	}{{"--stop-timeout", f.stopTimeout, &cfg.StopTimeout}, {"--idle-timeout", f.idleTimeout, &cfg.IdleTimeout}, {"--write-timeout", f.writeTimeout, &cfg.WriteTimeout}} {
		if *d.dst, err = parseTimeout(d.v); err != nil {
			return cfg, fmt.Errorf("%s: %w", d.name, err)
		}
	}
	return cfg, cfg.Validate()
}

// parseProtocols reads a comma-separated subset of s3, gcs and azure.
func parseProtocols(v string) ([]string, error) {
	var out []string
	for _, p := range strings.Split(v, ",") {
		switch p := strings.ToLower(strings.TrimSpace(p)); p {
		case server.ProtocolS3, server.ProtocolGCS, server.ProtocolAzure:
			out = append(out, p)
		default:
			return nil, errors.New("--protocols must be a comma-separated subset of s3,gcs,azure")
		}
	}
	return out, nil
}

// parseTimeout reads 500ms, 10s or an ISO-8601 duration (PT10S, P1DT2H).
func parseTimeout(v string) (time.Duration, error) {
	t := strings.ToLower(strings.TrimSpace(v))
	bad := fmt.Errorf("invalid serve timeout: %s", v)
	switch {
	case strings.HasPrefix(t, "p"), strings.HasPrefix(t, "-p"), strings.HasPrefix(t, "+p"):
		return parseISODuration(t, bad)
	case strings.HasSuffix(t, "ms"):
		n, err := strconv.ParseInt(t[:len(t)-2], 10, 64)
		if err != nil {
			return 0, bad
		}
		return time.Duration(n) * time.Millisecond, nil
	case strings.HasSuffix(t, "s"):
		n, err := strconv.ParseInt(t[:len(t)-1], 10, 64)
		if err != nil {
			return 0, bad
		}
		return time.Duration(n) * time.Second, nil
	}
	return parseISODuration(t, bad)
}

var isoDuration = regexp.MustCompile(`^([-+]?)p(?:([-+]?\d+)d)?(?:t(?:([-+]?\d+)h)?(?:([-+]?\d+)m)?(?:([-+]?\d+(?:[.,]\d{0,9})?)s)?)?$`)

func parseISODuration(t string, bad error) (time.Duration, error) {
	m := isoDuration.FindStringSubmatch(t)
	if m == nil || strings.HasSuffix(t, "t") || t == "p" || t == "-p" || t == "+p" {
		return 0, bad
	}
	var d float64
	for i, unit := range []float64{0, 0, 86400, 3600, 60, 1} {
		if i < 2 || m[i] == "" {
			continue
		}
		n, err := strconv.ParseFloat(strings.ReplaceAll(m[i], ",", "."), 64)
		if err != nil {
			return 0, bad
		}
		d += n * unit
	}
	if m[1] == "-" {
		d = -d
	}
	if math.Abs(d) > math.MaxInt64/1e9 {
		return 0, bad
	}
	return time.Duration(d * 1e9), nil
}

var hostLabel = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$`)

// validAdvertisedHost accepts a DNS name, a dotted IPv4 address without
// leading zeros, or a bracketed or bare IPv6 address, never a wildcard
// address, scheme, port, path, user or zone.
func validAdvertisedHost(v string) bool {
	if strings.TrimSpace(v) == "" || strings.ContainsAny(v, "/@?%") {
		return false
	}
	h := v
	if strings.HasPrefix(v, "[") && strings.HasSuffix(v, "]") {
		h = v[1 : len(v)-1]
	}
	if strings.Contains(h, ":") {
		ip := net.ParseIP(h)
		return ip != nil && ip.To4() == nil && !ip.IsUnspecified() && strings.Count(h, ":") >= 2 &&
			!strings.Contains(strings.ToLower(h), "ffff:")
	}
	if strings.ContainsAny(v, "[]") || len(v) > 253 || v == "0.0.0.0" {
		return false
	}
	if strings.Trim(v, "0123456789.") == "" && strings.Contains(v, ".") {
		octets := strings.Split(v, ".")
		if len(octets) != 4 {
			return false
		}
		for _, o := range octets {
			n, err := strconv.Atoi(o)
			if err != nil || len(o) > 3 || (len(o) > 1 && o[0] == '0') || n > 255 {
				return false
			}
		}
	}
	for _, label := range strings.Split(v, ".") {
		if len(label) > 63 || !hostLabel.MatchString(label) {
			return false
		}
	}
	return true
}

// startupLine is the line a launcher waits for: the bound endpoints and the
// resolved serving configuration, in the replay wire contract's key=value
// shape.
func startupLine(f *serveFlags, srv *server.Server, cfg server.Config, readers int) string {
	inject := "off"
	if cfg.Latency != nil {
		inject = f.injectLatency
	}
	metricsEndpoint := "off"
	if p := srv.MetricsPort(); p >= 0 {
		metricsEndpoint = server.MetricsEndpoint(f.host, p)
	}
	fixture, err := filepath.Abs(f.fixture)
	if err != nil {
		fixture = f.fixture
	}
	return fmt.Sprintf("swath_replay endpoint=http://%s:%d bucket=%s fixture=%s serving_mode=%s parquet_connections=%d "+
		"inject_latency=%s latency_scale=%s metrics_endpoint=%s max_concurrent_requests=%d protocols=%s "+
		"response_buffer_budget=%d max_response_bytes=%d max_responses=%d output_chunk_bytes=%d fixture_identity=%s "+
		"ordering_profile=%s metadata_policy=fixture-plus-synthetic profiles=%s native_endpoints=%s",
		f.host, srv.Port(), f.bucket, fixture, cfg.ServingMode, readers, inject, javanum.FormatDouble(f.latencyScale),
		metricsEndpoint, cfg.MaxConcurrentRequests, cfg.ProtocolList(), cfg.ResponseBufferBudget, cfg.MaxResponseBytes, cfg.MaxResponses(),
		server.OutputChunkBytes, cfg.FixtureIdentity, cfg.OrderingProfile(), cfg.ProfilesString(),
		nativeEndpoints(f, cfg, srv.Port()))
}

// listIndexLine reports the listing index build, after the startup line:
// its state (built, off, unsupported for a fixture that is not a
// SeekParquet dataset, failed), the budget and workers, the module's build
// stats (has_index: any row group indexed), and a failed build's error. A failed build leaves
// the server serving without the index.
func listIndexLine(b store.ListIndexBuild) string {
	state := b.State
	if state == "" {
		state = store.ListIndexUnsupported
	}
	st := b.Stats
	line := fmt.Sprintf("swath_replay list_index state=%s max_bytes=%d workers=%d groups_indexed=%d groups_skipped=%d "+
		"groups_failed=%d parts_failed=%d pages=%d entries=%d bytes=%d budget_exhausted=%t has_index=%t seconds=%s",
		state, b.MaxBytes, b.Workers, st.GroupsIndexed, st.GroupsSkipped, st.GroupsFailed, st.PartsFailed, st.Pages,
		st.Entries, st.Bytes, st.BudgetExhausted, st.HasIndex, strconv.FormatFloat(st.Duration.Seconds(), 'f', 3, 64))
	if b.Err != nil {
		line += " error=" + strconv.Quote(b.Err.Error())
	}
	return line
}

// nativeEndpoints is one example URL per enabled route, S3 first.
func nativeEndpoints(f *serveFlags, cfg server.Config, port int) string {
	base := "http://" + bracketHost(f.advertisedHost, f.host) + ":" + strconv.Itoa(port)
	var out []string
	for _, p := range cfg.Protocols {
		if p == server.ProtocolS3 {
			out = append(out, "s3:"+base+"/"+f.bucket+"?list-type=2")
		}
	}
	for _, p := range cfg.Protocols {
		if p == server.ProtocolGCS {
			out = append(out, "gcs:"+base+"/storage/v1/b/"+f.bucket+"/o")
		}
	}
	for _, p := range cfg.Protocols {
		if p == server.ProtocolAzure {
			out = append(out, "azure:"+base+"/"+cfg.AzureAccount+"/"+f.bucket+"?restype=container&comp=list")
		}
	}
	return strings.Join(out, ",")
}

func bracketHost(advertised, host string) string {
	if advertised != "" {
		host = advertised
	}
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		return "[" + host + "]"
	}
	return host
}
