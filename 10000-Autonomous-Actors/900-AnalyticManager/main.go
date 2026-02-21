package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/storage"
	"connectrpc.com/connect"
	"github.com/moby/moby/client"
	"github.com/redis/go-redis/v9"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	_ "github.com/lib/pq" // Postgres Driver

	whisper "Olympus2/90000-Enablement-Labs/P0000-pkg/000-whisper"
	analyticv1 "OlympusGCP-Data-AI/40000-Communication-Contracts/430-Protocol-Definitions/000-gen/analytic/v1"
	"OlympusGCP-Data-AI/40000-Communication-Contracts/430-Protocol-Definitions/000-gen/analytic/v1/analyticv1connect"
)

type AnalyticServer struct {
	storageClient   *storage.Client
	firestoreClient *firestore.Client
	bigqueryClient  *bigquery.Client
	dockerClient    *client.Client
	redisClient     *redis.Client
	ollamaURL       string
	logger          *whisper.WhisperLog
}

// --- Cloud Storage (GCS) ---

func (s *AnalyticServer) GetDownloadURL(ctx context.Context, req *connect.Request[analyticv1.GCSRequest]) (*connect.Response[analyticv1.GCSResponse], error) {
	host := os.Getenv("STORAGE_EMULATOR_HOST")
	url := fmt.Sprintf("http://%s/%s/%s", host, req.Msg.Bucket, req.Msg.Name)
	return connect.NewResponse(&analyticv1.GCSResponse{Url: url}), nil
}

func (s *AnalyticServer) UploadObject(ctx context.Context, req *connect.Request[analyticv1.UploadRequest]) (*connect.Response[analyticv1.UploadResponse], error) {
	wc := s.storageClient.Bucket(req.Msg.Bucket).Object(req.Msg.Name).NewWriter(ctx)
	wc.Write(req.Msg.Data)
	wc.Close()
	return connect.NewResponse(&analyticv1.UploadResponse{Path: req.Msg.Bucket + "/" + req.Msg.Name}), nil
}

// --- NoSQL / Firestore ---

func (s *AnalyticServer) Upsert(ctx context.Context, req *connect.Request[analyticv1.UpsertRequest]) (*connect.Response[analyticv1.StatusResponse], error) {
	var data map[string]interface{}
	json.Unmarshal([]byte(req.Msg.DataJson), &data)
	s.firestoreClient.Collection(req.Msg.Collection).Doc(req.Msg.DocId).Set(ctx, data)
	return connect.NewResponse(&analyticv1.StatusResponse{Success: true}), nil
}

func (s *AnalyticServer) QueryData(ctx context.Context, req *connect.Request[analyticv1.QueryRequest]) (*connect.Response[analyticv1.JSONResponse], error) {
	iter := s.firestoreClient.Collection(req.Msg.Collection).Documents(ctx)
	var results []map[string]interface{}
	for {
		doc, err := iter.Next()
		if err == iterator.Done { break }
		results = append(results, doc.Data())
	}
	out, _ := json.Marshal(results)
	return connect.NewResponse(&analyticv1.JSONResponse{Json: string(out), Count: int64(len(results))}), nil
}

// --- Analytics / BigQuery ---

func (s *AnalyticServer) QueryBigQuery(ctx context.Context, req *connect.Request[analyticv1.BigQueryRequest]) (*connect.Response[analyticv1.JSONResponse], error) {
	q := s.bigqueryClient.Query(req.Msg.Query)
	it, _ := q.Read(ctx)
	var results []map[string]bigquery.Value
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done { break }
		results = append(results, row)
	}
	out, _ := json.Marshal(results)
	return connect.NewResponse(&analyticv1.JSONResponse{Json: string(out), Count: int64(len(results))}), nil
}

// --- Relational / Cloud SQL ---

func (s *AnalyticServer) ExecuteSQL(ctx context.Context, req *connect.Request[analyticv1.SQLRequest]) (*connect.Response[analyticv1.JSONResponse], error) {
	db, _ := sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	defer db.Close()
	rows, _ := db.QueryContext(ctx, req.Msg.Query)
	defer rows.Close()
	// var results []map[string]interface{}
	// Scan logic omitted for brevity in consolidation phase
	return connect.NewResponse(&analyticv1.JSONResponse{Json: "[]", Count: 0}), nil
}

// --- Cache / MemoryStore ---

func (s *AnalyticServer) SetCache(ctx context.Context, req *connect.Request[analyticv1.CacheRequest]) (*connect.Response[analyticv1.StatusResponse], error) {
	s.redisClient.Set(ctx, req.Msg.Key, req.Msg.Value, 0)
	return connect.NewResponse(&analyticv1.StatusResponse{Success: true}), nil
}

func (s *AnalyticServer) GetCache(ctx context.Context, req *connect.Request[analyticv1.CacheRequest]) (*connect.Response[analyticv1.JSONResponse], error) {
	val, _ := s.redisClient.Get(ctx, req.Msg.Key).Result()
	return connect.NewResponse(&analyticv1.JSONResponse{Json: val, Count: 1}), nil
}

// --- AI / Vertex / Ollama ---

func (s *AnalyticServer) Predict(ctx context.Context, req *connect.Request[analyticv1.PredictRequest]) (*connect.Response[analyticv1.PredictResponse], error) {
	ollamaReq := map[string]interface{}{"model": req.Msg.Model, "prompt": req.Msg.Prompt, "stream": false}
	body, _ := json.Marshal(ollamaReq)
	resp, _ := http.Post(s.ollamaURL+"/api/generate", "application/json", bytes.NewBuffer(body))
	var res struct { Response string `json:"response"` }
	json.NewDecoder(resp.Body).Decode(&res)
	return connect.NewResponse(&analyticv1.PredictResponse{Prediction: res.Response}), nil
}

func (s *AnalyticServer) Embed(ctx context.Context, req *connect.Request[analyticv1.EmbedRequest]) (*connect.Response[analyticv1.EmbedResponse], error) {
	return connect.NewResponse(&analyticv1.EmbedResponse{Values: []float32{0.1, 0.2}}), nil
}

func (s *AnalyticServer) SpannerQuery(ctx context.Context, req *connect.Request[analyticv1.SpannerRequest]) (*connect.Response[analyticv1.JSONResponse], error) {
	return connect.NewResponse(&analyticv1.JSONResponse{Json: "[]", Count: 0}), nil
}

func main() {
	slog.Info("AnalyticManager: Booting Data & AI Substrate...")
	w := whisper.New("AnalyticManager", "gcp_analytic.lpsv")
	defer w.Close()

	ctx := context.Background()
	projectID := "olympus-project"

	// Init Clients (Simplified for YOLO)
	sClient, _ := storage.NewClient(ctx, option.WithoutAuthentication())
	fsClient, _ := firestore.NewClient(ctx, projectID, option.WithoutAuthentication())
	bqClient, _ := bigquery.NewClient(ctx, projectID, option.WithoutAuthentication())
	dockerCli, _ := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	redisCli := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	server := &AnalyticServer{
		storageClient:   sClient,
		firestoreClient: fsClient,
		bigqueryClient:  bqClient,
		dockerClient:    dockerCli,
		redisClient:     redisCli,
		ollamaURL:       "http://localhost:11434",
		logger:          w,
	}

	mux := http.NewServeMux()
	mux.Handle(analyticv1connect.NewAnalyticServiceHandler(server))

	port := "8093"
	slog.Info("AnalyticManager: Listening...", "addr", "localhost:"+port)
	http.ListenAndServe("localhost:"+port, h2c.NewHandler(mux, &http2.Server{}))
}
