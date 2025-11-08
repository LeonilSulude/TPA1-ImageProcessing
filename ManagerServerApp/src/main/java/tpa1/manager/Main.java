package tpa1.manager;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import manager_server.ManagerServerRegistrationServiceGrpc;
import manager_server.ManagerServer.RegisterImgServerRequest;
import manager_server.ManagerServer.RegisterImgServerResponse;
import manager_client.ManagerServerClientServiceGrpc;
import manager_client.ManagerClient.GetImgServerRequest;
import manager_client.ManagerClient.GetImgServerResponse;

import io.grpc.stub.StreamObserver;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getProperty("port", "50051"));
        int redisPort = Integer.parseInt(System.getProperty("redisPort", "6379"));
        String managerIp = System.getProperty("managerIp", "127.0.0.1");

        ImgRegistry registry = new ImgRegistry(managerIp, redisPort);

        Server server = ServerBuilder.forPort(port)
                .addService(new RegService(registry))
                .addService(new DistService(registry))
                .build()
                .start();

        System.out.printf("ManagerServer a correr em %s:%d (RedisPort=%d)\n", managerIp, port, redisPort);
        server.awaitTermination();
    }

    // Guarda ImgServers e fornece round-robin
    static class ImgRegistry {
        private final String managerIp; // IP onde também está o Redis
        private final int redisPort;
        private final List<Endpoint> servers = Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger rr = new AtomicInteger(0);

        ImgRegistry(String managerIp, int redisPort) { this.managerIp = managerIp; this.redisPort = redisPort; }

        void register(String ip, int port) {
            servers.add(new Endpoint(ip, port));
            System.out.printf("[Manager] Registado ImgServer %s:%d\n", ip, port);
        }

        int getRedisPort() { return redisPort; }
        String getRedisIp() { return managerIp; }

        Endpoint next() {
            if (servers.isEmpty()) throw new IllegalStateException("Sem ImgServers registados");
            int i = Math.abs(rr.getAndIncrement() % servers.size());
            return servers.get(i);
        }
    }

    record Endpoint(String ip, int port) {}

    // Service: ManagerServerRegistrationService
    static class RegService extends ManagerServerRegistrationServiceGrpc.ManagerServerRegistrationServiceImplBase {
        private final ImgRegistry reg;
        RegService(ImgRegistry reg){ this.reg = reg; }

        @Override
        public void registerImgServer(RegisterImgServerRequest request, StreamObserver<RegisterImgServerResponse> responseObserver) {
            reg.register(request.getImgServerIp(), request.getImgServerPort());
            RegisterImgServerResponse resp = RegisterImgServerResponse.newBuilder()
                    .setRedisPort(reg.getRedisPort())
                    .setStatus("OK")
                    .build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }
    }

    // Service: ManagerServerClientService
    static class DistService extends ManagerServerClientServiceGrpc.ManagerServerClientServiceImplBase {
        private final ImgRegistry reg;
        DistService(ImgRegistry reg){ this.reg = reg; }

        @Override
        public void getImgServer(GetImgServerRequest request, StreamObserver<GetImgServerResponse> responseObserver) {
            Endpoint e = reg.next();
            GetImgServerResponse resp = GetImgServerResponse.newBuilder()
                    .setImgServerIp(e.ip())
                    .setImgServerPort(e.port())
                    .build();
            System.out.printf("[Manager] Atribuído ImgServer %s:%d ao cliente\n", e.ip(), e.port());
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }
    }
}