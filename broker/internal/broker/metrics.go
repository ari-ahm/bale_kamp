package broker

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"time"
)

var (
	activeSubscribers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "broker_active_subscribers",
		Help: "Number of active subscribers",
	})

	methodCalls = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "broker_method_calls",
		Help: "Method call duration and status",
	}, []string{"method", "status", "error"})
)

func InitMetrics() {
	prometheus.MustRegister(activeSubscribers)
	prometheus.MustRegister(methodCalls)
}

func MetricsUnaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()
	ret, err := handler(ctx, req)
	duration := time.Since(start).Milliseconds()

	status := "successful"
	errorLabel := ""
	if err != nil {
		status = "failed"
		errorLabel = err.Error()
	}

	methodCalls.With(prometheus.Labels{
		"method": info.FullMethod,
		"status": status,
		"error":  errorLabel,
	}).Observe(float64(duration))

	return ret, err
}

func MetricsStreamInterceptor(
	srv any, ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	switch info.FullMethod {
	case "/broker.Broker/Subscribe":
		activeSubscribers.Inc()
	}

	start := time.Now()
	err := handler(srv, ss)
	duration := time.Since(start).Milliseconds()

	switch info.FullMethod {
	case "/broker.Broker/Subscribe":
		activeSubscribers.Dec()
	}

	statusLabel := "successful"
	errorLabel := ""
	if err != nil {
		statusLabel = "failed"
		errorLabel = status.Convert(err).String()
	}

	methodCalls.With(prometheus.Labels{
		"method": info.FullMethod,
		"status": statusLabel,
		"error":  errorLabel,
	}).Observe(float64(duration))

	return err
}
