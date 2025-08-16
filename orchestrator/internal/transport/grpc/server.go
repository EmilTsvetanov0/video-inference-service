package grpc

import (
	"context"
	"errors"
	orcv1 "github.com/Emiltsvetanov0/video-inference-service/api/gen/go/orchestrator/v1"
	"log"
	"orchestrator/internal/runners"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	orcv1.UnimplementedRunnerControlServer
	runnerPool *runners.ScenarioPool
}

type Response struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func New(rp *runners.ScenarioPool) *Server {
	return &Server{
		runnerPool: rp,
	}
}

func (s *Server) Terminate(ctx context.Context, req *orcv1.TerminateRunnerRequest) (*orcv1.TerminateRunnerResponse, error) {

	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}

	if s.runnerPool == nil {
		log.Println("[orchestrator] runnerPool is nil")
		return nil, status.Error(codes.Internal, "runnerPool is nil")
	}

	log.Printf("[orchestrator] Stopping runner %s because of the error: %v", req.Id, req.Error)

	if err := s.runnerPool.StopScenario(ctx, req.Id); err != nil {
		log.Printf("[orchestrator] Error terminating runner %s: %v", req.Id, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	log.Println("[orchestrator] Runner stopped successfully")

	return &orcv1.TerminateRunnerResponse{Message: "ok"}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *orcv1.HeartbeatRequest) (*emptypb.Empty, error) {

	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}

	if s.runnerPool == nil {
		log.Println("[orchestrator] runnerPool is nil")
		return nil, status.Error(codes.Internal, "runnerPool is nil")
	}

	if err := s.runnerPool.AcceptHeartbeat(req.Id); err != nil {
		log.Printf("[orchestrator] Failed to accept heartbeat: %v", err)
		if errors.Is(err, runners.ScenarioNotFoundErr) {
			return nil, status.Error(codes.NotFound, "scenario not found")
		} else if errors.Is(err, runners.ScenarioNotActiveErr) {
			return nil, status.Error(codes.NotFound, "scenario not active")
		} else {
			return nil, status.Error(codes.Internal, "internal server error")
		}
	}

	return &emptypb.Empty{}, nil
}
