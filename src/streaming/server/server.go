package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	pb "streaming/proto"
	"time"

	"google.golang.org/grpc"
)

type server struct {
}

const (
	port = "6001"
)

func main() {

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	// if *tls {
	// 	if *certFile == "" {
	// 		*certFile = testdata.Path("server1.pem")
	// 	}
	// 	if *keyFile == "" {
	// 		*keyFile = testdata.Path("server1.key")
	// 	}
	// 	creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
	// 	if err != nil {
	// 		log.Fatalf("Failed to generate credentials %v", err)
	// 	}
	// 	opts = []grpc.ServerOption{grpc.Creds(creds)}
	// }
	grpcServer := grpc.NewServer(opts...)
	//pb.RegisterRouteGuideServer(grpcServer, newServer())
	pb.RegisterRequestServiceServer(grpcServer, &server{})
	grpcServer.Serve(lis)
}

func (s *server) HandleStream(rect *pb.StreamInput, stream pb.RequestService_HandleStreamServer) error {
	dataQueue := make(chan string, 1)
	go readFile(rect.FileName, dataQueue)
	for item := range dataQueue {
		if err := stream.Send(&pb.StreamOutput{Data: item}); err != nil {
			return err
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}

func readFile(filePath string, outputQueue chan<- string) {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = f.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				time.Sleep(1 * time.Second)
				//log.Println("Waiting for more lines")
			} else {
				break
			}
		}
		if line != "" {
			outputQueue <- line
		}

	}
}
