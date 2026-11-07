package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Trendyzima/federated-content/internal/backend"
	"github.com/Trendyzima/federated-content/internal/config"
	"github.com/Trendyzima/federated-content/internal/httpapi"
	"github.com/Trendyzima/federated-content/internal/store"
	"github.com/Trendyzima/federated-content/internal/worker"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--healthcheck" {
		client := &http.Client{Timeout: 3 * time.Second}
		response, err := client.Get("http://127.0.0.1:8081/readyz")
		if err != nil {
			os.Exit(1)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		return
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(1)
	}

	var federationStore *store.Store
	if cfg.FederationDatabaseURL != "" {
		federationStore, err = store.Open(context.Background(), cfg.FederationDatabaseURL)
		if err != nil {
			logger.Error("federation storage initialization failed", "error", err)
			os.Exit(1)
		}
		defer federationStore.Close()
	}
	var backendClient *backend.Client
	if cfg.BackendInternalURL != "" {
		backendClient, err = backend.New(cfg.BackendInternalURL, cfg.BackendInternalToken, nil)
		if err != nil {
			logger.Error("Backend federation client configuration failed", "error", err)
			os.Exit(1)
		}
	}

	var deliveryWorker *worker.DeliveryWorker
	if cfg.DeliveryWorkerEnabled {
		if federationStore == nil {
			logger.Error("delivery worker requires FEDERATION_DATABASE_URL")
			os.Exit(1)
		}
		deliveryWorker, err = worker.NewDeliveryWorker(federationStore, nil, logger)
		if err != nil {
			logger.Error("delivery worker initialization failed", "error", err)
			os.Exit(1)
		}
	}
	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.NewWithRuntime(cfg, logger, federationStore, backendClient),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if deliveryWorker != nil {
		go deliveryWorker.Run(ctx)
		logger.Info("federation outbound delivery worker enabled")
	}

	go func() {
		logger.Info("federation API starting", "addr", cfg.HTTPAddr, "base_url", cfg.PublicBaseURL)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		_ = server.Close()
	}
	logger.Info("federation API stopped")
}
