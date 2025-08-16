package runner

import (
	"context"
	"encoding/json"
	orcv1 "github.com/Emiltsvetanov0/video-inference-service/api/gen/go/orchestrator/v1"
	"github.com/spf13/viper"
	"gocv.io/x/gocv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"producer/internal/cconfig"
	"producer/internal/domain"
	"producer/internal/kafka"
	"sync"
	"time"
)

var (
	hbInterval        = 3 * time.Second
	orchGRPCAddr      = "127.0.0.1:9090"
	hbTries           = 5
	frameSendInterval = 3000 * time.Millisecond
	grpcOnce          sync.Once
	grpcConn          *grpc.ClientConn
	grpcCli           orcv1.RunnerControlClient
	grpcMux           sync.Mutex
)

const videoFolder = "./files/"

func init() {
	cconfig.InitConfig()

	hbInterval = time.Duration(viper.GetInt("runner.heartbeat_interval")) * time.Second
	frameSendInterval = time.Duration(viper.GetInt("runner.frame_interval_millis")) * time.Millisecond
	orchGRPCAddr = viper.GetString("runner.orchestrator_grpc_addr")
	hbTries = viper.GetInt("runner.heartbeat_tries")
}

func ensureGRPC(ctx context.Context) error {
	var dialErr error
	grpcOnce.Do(func() {
		grpcMux.Lock()
		defer grpcMux.Unlock()

		if grpcConn != nil {
			return
		}
		conn, err := grpc.DialContext(
			ctx,
			orchGRPCAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		if err != nil {
			dialErr = err
		}
		grpcConn = conn
		grpcCli = orcv1.NewRunnerControlClient(grpcConn)
	})
	return dialErr
}

func resetGRPC() {
	grpcMux.Lock()
	defer grpcMux.Unlock()
	if grpcConn != nil {
		_ = grpcConn.Close()
		grpcConn = nil
		grpcCli = nil
	}

	grpcOnce = sync.Once{}
}

type Runner struct {
	active  bool
	mutex   sync.RWMutex
	jobName string
}

func NewRunner(name string) *Runner {
	return &Runner{
		active:  false,
		jobName: name,
	}
}

func (r *Runner) isActive() bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.active
}

func (r *Runner) Start(ctx context.Context) bool {
	log.Printf("[producer] [Runner.Start] Runner %s starting", r.jobName)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.active {
		return false
	}
	r.active = true

	// Здесь отправляем хартбиты

	startFrames := make(chan struct{}, 1)

	go func(job string) {
		defer close(startFrames)
		started := false

		currentTries := 0
		ticker := time.NewTicker(hbInterval)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				log.Println("[producer] [Runner.Start] Heartbeat stopped")
				return

			case <-ticker.C:
				if !r.isActive() {
					log.Println("[producer] [Runner.Start] Stopping heartbeat sending due to runner inactivity")
					return
				}

				if grpcCli == nil {
					dctx, cancel := context.WithTimeout(ctx, 3*time.Second)
					err := ensureGRPC(dctx)
					cancel()
					if err != nil {
						log.Printf("[producer] [Runner.Start] Heartbeat dial error: %v", err)
						currentTries++
						if currentTries >= hbTries {
							log.Println("[producer] [Runner.Start] Heartbeat dial tries limit reached, cancelling runner")
							r.Stop()
							return
						}
						continue
					}
				}

				log.Printf("[producer] [Runner.Start] Sending heartbeat for %s", r.jobName)
				hctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				_, err := grpcCli.Heartbeat(hctx, &orcv1.HeartbeatRequest{
					Id:        job,
					Timestamp: time.Now().Unix(),
				})
				cancel()

				if err != nil {
					log.Printf("[producer] [Runner.Start] Heartbeat error: %v", err)
					currentTries++
					if currentTries >= hbTries {
						log.Println("[producer] [Runner.Start] Heartbeat tries limit reached, cancelling runner")
						r.Stop()
						return
					}
					continue
				}

				if !started {
					if !started {
						started = true
						startFrames <- struct{}{}
					}
					currentTries = 0
				}
			}
		}
	}(r.jobName)

	go func(job string) {

		select {
		case <-ctx.Done():
			log.Println("[producer] [Runner.Start] Frame sender: context canceled before start")
			return
		case <-startFrames:
			log.Println("[producer] [Runner.Start] Frame sender: starting after successful heartbeat")
		}

		video, err := gocv.VideoCaptureFile(videoFolder + job + ".mp4")
		if err != nil {
			log.Println("[producer] [Runner.Start] VideoCaptureFile error:", err)
			NotifyAboutStopping(ctx, job, err)
			r.Stop()
			return
		}
		defer video.Close()

		img := gocv.NewMat()
		defer img.Close()

		ticker := time.NewTicker(frameSendInterval)

		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				log.Println("[producer] [Runner.Start] Frame sender stopped")
				return
			case <-ticker.C:
				if !r.isActive() {
					log.Println("[producer] [Runner.Start] Stopping frames sending due to runner inactivity")
					return
				}

				if !video.Read(&img) || img.Empty() {
					video.Close()
					video, err = gocv.VideoCaptureFile(job + ".mp4")
					if err != nil {
						log.Println("[producer] [Runner.Start] VideoCaptureFile error:", err)
						NotifyAboutStopping(ctx, job, err)
						r.Stop()
						return
					}
					continue
				}

				//frameData := img.ToBytes()

				buf, err := gocv.IMEncode(".jpg", img)
				if err != nil {
					log.Printf("[producer] [Runner.Start] failed to encode image: %v", err)
					r.Stop()
					return
				}
				frameData := buf.GetBytes()
				buf.Close()

				msg, err := json.Marshal(domain.VideoFrame{
					ScenarioId: r.jobName,
					Data:       frameData,
				})
				if err != nil {
					log.Print("[producer] [Runner.Start] Unmarshalling error: ", err)
					r.Stop()
					return
				}
				kafka.PushImageToQueue(job, msg)
			}
		}
	}(r.jobName)
	return true
}

func (r *Runner) Stop() bool {
	log.Printf("[producer] [Runner.Stop] Runner %s stopping", r.jobName)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if !r.active {
		return false
	}
	r.active = false
	return true
}

// Runner pool

type Pool struct {
	mu   sync.Mutex
	pool map[string]*Runner
}

func NewPool() *Pool {
	return &Pool{
		pool: make(map[string]*Runner),
	}
}

func (p *Pool) Start(ctx context.Context, id string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	r, ok := p.pool[id]
	if !ok {
		r = NewRunner(id)
		p.pool[id] = r
	}

	if !r.Start(ctx) {
		log.Println("[producer] Runner " + id + " already started")
	} else {
		log.Println("[producer] Runner " + id + " started")
	}
}

func (p *Pool) Stop(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	r, ok := p.pool[id]
	if !ok {
		log.Println("[producer] Runner " + id + " doesn't exist")
		return
	}

	if !r.Stop() {
		log.Println("[producer] Runner " + id + " already stopped")
	} else {
		log.Println("[producer] Runner " + id + " stopped")
	}
}

func NotifyAboutStopping(ctx context.Context, job string, err error) {
	log.Println("[producer] [NotifyAboutStopping]", err)

	if grpcCli == nil {
		dctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_ = ensureGRPC(dctx)
		cancel()
	}

	if grpcCli == nil {
		log.Printf("[producer] [NotifyAboutStopping] no gRPC client to notify (addr %s)", orchGRPCAddr)
		return
	}

	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, callErr := grpcCli.Terminate(tctx, &orcv1.TerminateRunnerRequest{
		Id:        job,
		Timestamp: time.Now().Unix(),
		Error:     err.Error(),
	})

	if callErr != nil {
		log.Printf("[producer] [NotifyAboutStopping] gRPC terminate error: %v", callErr)
		// сбросим, чтобы следующая попытка могла переподключиться
		resetGRPC()
		return
	}

	log.Printf("[producer] [NotifyAboutStopping] notified orchestrator about stop of %s", job)
}
