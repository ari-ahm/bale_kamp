package broker

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"time"
)

var (
	activeSubscribers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "broker_active_subscribers",
		Help: "Number of active subscribers",
	})

	methodCalls = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "broker_method_calls",
		Help:    "Method call duration and status",
		Buckets: []float64{100, 200, 300, 400, 500, 750, 1000, 1250, 1500, 1750, 2000, 2500, 3000, 3500, 4000, 4500, 5000},
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

	err := handler(srv, ss)

	switch info.FullMethod {
	case "/broker.Broker/Subscribe":
		activeSubscribers.Dec()
	}

	return err
}
