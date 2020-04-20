package server

import (
	"net"
	"net/http"
	"strconv"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/internal/api"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/internal/bundles"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/internal/db"
	"github.com/sourcegraph/sourcegraph/internal/trace/ot"
)

type Server struct {
	host                string
	port                int
	db                  db.DB
	bundleManagerClient bundles.BundleManagerClient
	api                 api.CodeIntelAPI
}

type ServerOpts struct {
	Host                string
	Port                int
	DB                  db.DB
	BundleManagerClient bundles.BundleManagerClient
}

func New(opts ServerOpts) *Server {
	return &Server{
		host:                opts.Host,
		port:                opts.Port,
		db:                  opts.DB,
		bundleManagerClient: opts.BundleManagerClient,
		api:                 api.New(opts.DB, opts.BundleManagerClient),
	}
}

func (s *Server) Start() error {
	addr := net.JoinHostPort(s.host, strconv.FormatInt(int64(s.port), 10))
	handler := ot.Middleware(s.handler())
	server := &http.Server{Addr: addr, Handler: handler}

	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		// TODO - should fatal instead
		return err
	}

	return nil
}
