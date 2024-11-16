package serie

import (
	"context"

	"github.com/nireo/serie/pb"
)

type grpcConfig struct {
	engine Engine
}

type grpcServer struct {
	*grpcConfig
	*pb.UnimplementedSerieServer
}

func newGrpcServer(config *grpcConfig) (srv *grpcServer, err error) {
	srv = &grpcServer{
		config,
		&pb.UnimplementedSerieServer{},
	}
	return srv, nil
}

func convertToNonPbPoints(pbPoints []*pb.Point) []Point {
	points := make([]Point, 0, len(pbPoints))
	for _, p := range pbPoints {
		points = append(points, Point{
			Metric:    p.Metric,
			Value:     p.Value,
			Timestamp: p.Timestamp,
			Tags:      p.Tags,
		})
	}

	return points
}

func convertToPbPoints(points []Point) []*pb.Point {
	pbPoints := make([]*pb.Point, 0, len(points))
	for _, p := range pbPoints {
		pbPoints = append(pbPoints, &pb.Point{
			Metric:    p.Metric,
			Value:     p.Value,
			Timestamp: p.Timestamp,
			Tags:      p.Tags,
		})
	}

	return pbPoints
}

func (s *grpcServer) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	points := convertToNonPbPoints(req.Points)
	err := s.engine.WriteBatch(points)
	if err != nil {
		return nil, err
	}

	return &pb.WriteResponse{}, nil
}

func (s *grpcServer) WriteStream(stream pb.Serie_WriteStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		res, err := s.Write(stream.Context(), req)
		if err != nil {
			return err
		}

		if err := stream.Send(res); err != nil {
			return nil
		}
	}
}

func (s *grpcServer) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	points, err := s.engine.Read(req.TsQuery.Metric, req.TsQuery.MinTimestamp, req.TsQuery.MaxTimestamp)
	if err != nil {
		return nil, err
	}

	return &pb.ReadResponse{
		Points: convertToPbPoints(points),
	}, nil
}

func (s *grpcServer) ReadStream(req *pb.ReadRequest, stream pb.Serie_ReadStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Read(stream.Context(), req)
			if err != nil {
				return err
			}

			if err = stream.Send(res); err != nil {
				return err
			}
		}
	}
}
