package tpa1.imgserver;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ManagedChannel;
import io.grpc.LoadBalancerRegistry;
import io.grpc.internal.PickFirstLoadBalancerProvider;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.net.InetSocketAddress;
import java.nio.file.*;

import manager_server.ManagerServer;
import manager_server.ManagerServerRegistrationServiceGrpc;

/**
 * Main do ImgServerApp
 * Responsável por:
 *  - Registar este ImgServer no ManagerServer
 *  - Iniciar servidor gRPC local (serviço ImgServerService)
 *  - Conectar ao Redis e preparar DockerLauncher
 */
public class Main {
    static boolean DEBUG = Boolean.parseBoolean(System.getProperty("debug", "false"));

    public static void main(String[] args) throws Exception {
        // === 1) Configura ambiente de rede (evita resolvers unix/npipe) ===
        System.setProperty("io.grpc.internal.DnsNameResolverProvider.enable_jndi", "false");
        System.setProperty("io.grpc.internal.DnsNameResolverProvider.enable_service_config", "false");
        System.setProperty("java.net.preferIPv4Stack", "true");
        debug("[DEBUG] Configuração de rede ajustada para TCP/DNS (ignora resolvers unix)");

        // === 2) Lê propriedades de execução ===
        String ip = System.getProperty("ip", "127.0.0.1");
        int port = Integer.parseInt(System.getProperty("port", "50052"));
        String managerIp = System.getProperty("managerIp", "127.0.0.1");
        int managerPort = Integer.parseInt(System.getProperty("managerPort", "50051"));

        System.out.printf("[ImgServer] A registar no ManagerServer %s:%d ...\n", managerIp, managerPort);

        // === 3) Força load balancer default do gRPC ===
        LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());
        debug("[DEBUG] Registrado manualmente PickFirstLoadBalancerProvider");

        // === 4) Conecta ao ManagerServer via TCP direto ===
        ManagedChannel ch = NettyChannelBuilder
                .forAddress(new InetSocketAddress(managerIp, managerPort))
                .usePlaintext()
                .build();

        var regStub = ManagerServerRegistrationServiceGrpc.newBlockingStub(ch);
        ManagerServer.RegisterImgServerResponse regResp = regStub.registerImgServer(
                ManagerServer.RegisterImgServerRequest.newBuilder()
                        .setImgServerIp(ip)
                        .setImgServerPort(port)
                        .build()
        );
        ch.shutdownNow();

        int redisPort = regResp.getRedisPort();
        String redisHost = managerIp; // Redis vive junto ao ManagerServer
        System.out.printf("[ImgServer] Registado com sucesso. Redis em %s:%d\n", redisHost, redisPort);

        // === 5) Cria diretórios do volume local (input/output) ===
        Path volBase = Paths.get(System.getProperty("volumeBase",
                System.getProperty("user.home") + "/shared_volume"));
        Path inputDir = volBase.resolve("input");
        Path outputDir = volBase.resolve("output");
        Files.createDirectories(inputDir);
        Files.createDirectories(outputDir);

        // === 6) Inicializa Docker e Redis ===
        DockerLauncher docker = new DockerLauncher();
        StateStore store = new StateStore(redisHost, redisPort);

        // === 7) Cria e inicia o servidor gRPC ===
        ImgServerService svc = new ImgServerService(ip, port, inputDir, outputDir, docker, store);
        Server server = ServerBuilder.forPort(port)
                .addService(svc)
                .build()
                .start();

        System.out.printf("[ImgServer] A escutar em %s:%d (Redis=%s:%d, volume=%s)\n",
                ip, port, redisHost, redisPort, volBase);

        server.awaitTermination();
    }

    static void debug(String msg) {
        if (DEBUG) System.out.println("[DEBUG] " + msg);
    }
}
