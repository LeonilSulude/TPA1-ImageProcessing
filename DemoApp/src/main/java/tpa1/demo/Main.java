package tpa1.demo;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import img_client.ImgClient.*;
import img_client.ImgServerClientServiceGrpc;
import manager_client.*;
import manager_client.ManagerClient.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Demo.jar — Simula múltiplos clientes a enviar imagens concorrentes para o sistema distribuído.
 *
 * Uso:
 *   java -DmanagerIp=<ip_manager> -DmanagerPort=<port_manager> -Dthreads=<n> -Duploads=<m> -jar Demo.jar
 *
 * Exemplo:
 *   java -DmanagerIp=35.187.4.84 -DmanagerPort=8000 -Dthreads=5 -Duploads=3 -jar Demo.jar
 *
 * -> Simula 5 clientes, cada um envia 3 vezes a imagem "sample.png" (embutida no JAR)
 */
public class Main {

    static boolean DEBUG = Boolean.parseBoolean(System.getProperty("debug", "false"));

    public static void main(String[] args) throws Exception {
        String managerIp = System.getProperty("managerIp", "127.0.0.1");
        int managerPort = Integer.parseInt(System.getProperty("managerPort", "8000"));
        int clients = Integer.parseInt(System.getProperty("clients", "5"));
        int uploads = Integer.parseInt(System.getProperty("uploadsPerClient", "3"));

        // Copia imagem do resources para ficheiro temporário
        Path tmpImage = extractSampleImage();

        System.out.printf("[Demo] Iniciando simulação com %d clientes, %d uploads cada.%n", clients, uploads);
        System.out.printf("[Demo] Usando ManagerServer em %s:%d%n", managerIp, managerPort);

        // === Forçar IPv4 e DNS-only resolvers ===
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("io.grpc.internal.DnsNameResolverProvider.enable_jndi", "false");
        System.setProperty("io.grpc.internal.DnsNameResolverProvider.enable_service_config", "false");

        try {
            io.grpc.NameResolverRegistry registry = io.grpc.NameResolverRegistry.getDefaultRegistry();

            // Regista manualmente o DNS resolver
            io.grpc.internal.DnsNameResolverProvider dnsProv = new io.grpc.internal.DnsNameResolverProvider();
            registry.register(dnsProv);
            debug("[DEBUG] Re-registrado DNS resolver");

            // Remove resolvers unix/npipe
            try {
                java.lang.reflect.Field field = registry.getClass().getDeclaredField("providers");
                field.setAccessible(true);
                Object provs = field.get(registry);
                if (provs instanceof java.util.List<?>) {
                    ((java.util.List<?>) provs).removeIf(p -> {
                        try {
                            String scheme = ((io.grpc.NameResolverProvider) p).getDefaultScheme();
                            if (scheme.equals("unix") || scheme.equals("npipe")) {
                                debug("[DEBUG] Removido NameResolver: " + scheme);
                                return true;
                            }
                        } catch (Throwable ignored) {}
                        return false;
                    });
                }
            } catch (Throwable t) {
                System.out.println("[WARN] Falha ao remover resolvers unix/npipe: " + t.getMessage());
            }

            // Regista manualmente o load balancer padrão
            io.grpc.LoadBalancerRegistry.getDefaultRegistry()
                    .register(new io.grpc.internal.PickFirstLoadBalancerProvider());
            debug("[DEBUG] Registrado manualmente PickFirstLoadBalancerProvider");

        } catch (Throwable t) {
            System.out.println("[WARN] Falha ao reforçar DNS resolver: " + t.getMessage());
        }

        ExecutorService pool = Executors.newFixedThreadPool(clients);
        List<Future<?>> futures = new ArrayList<>();

        long start = System.currentTimeMillis();

        for (int i = 1; i <= clients; i++) {
            int id = i;
            futures.add(pool.submit(() -> runClient(id, managerIp, managerPort, tmpImage, uploads)));
        }

        for (Future<?> f : futures) f.get();
        pool.shutdown();

        long elapsed = System.currentTimeMillis() - start;
        System.out.printf("[Demo] Todos os uploads concluídos em %.2f segundos.%n", elapsed / 1000.0);
    }

    private static void runClient(int clientId, String managerIp, int managerPort, Path imagePath, int uploads) {
        try {
            // === 1 - Obter ImgServer do ManagerServer ===
            ManagedChannel managerCh = NettyChannelBuilder.forAddress(managerIp, managerPort)
                    .usePlaintext()
                    .build();

            var mstub = ManagerServerClientServiceGrpc.newBlockingStub(managerCh);
            GetImgServerResponse sel = mstub.getImgServer(GetImgServerRequest.getDefaultInstance());
            managerCh.shutdownNow();

            String imgIp = sel.getImgServerIp();
            int imgPort = sel.getImgServerPort();

            System.out.printf("[Client#%d] Usando ImgServer %s:%d%n", clientId, imgIp, imgPort);

            // === 2 - Canal e stub gRPC ===
            ManagedChannel ch = NettyChannelBuilder.forAddress(imgIp, imgPort)
                    .usePlaintext()
                    .build();

            ImgServerClientServiceGrpc.ImgServerClientServiceStub stub = ImgServerClientServiceGrpc.newStub(ch);

            for (int i = 1; i <= uploads; i++) {
                String reqId = upload(clientId, stub, imagePath, i);
                System.out.printf("[Client#%d] Upload %d concluído -> requestId=%s%n", clientId, i, reqId);
            }

            ch.shutdownNow();
        } catch (Exception e) {
            System.err.printf("[Client#%d] ERRO: %s%n", clientId, e.getMessage());
        }
    }

    private static String upload(int clientId, ImgServerClientServiceGrpc.ImgServerClientServiceStub stub,
                                 Path file, int seq) throws Exception {
        final String[] rid = {null};

        StreamObserver<UploadImageResponse> respObs = new StreamObserver<>() {
            public void onNext(UploadImageResponse v) { rid[0] = v.getRequestId(); }
            public void onError(Throwable t) { System.err.printf("[Client#%d] Erro upload %d: %s%n", clientId, seq, t.getMessage()); }
            public void onCompleted() {}
        };

        StreamObserver<ImageChunk> reqObs = stub.uploadImage(respObs);
        try (InputStream in = Files.newInputStream(file)) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) != -1) {
                reqObs.onNext(ImageChunk.newBuilder()
                        .setData(com.google.protobuf.ByteString.copyFrom(buf, 0, n))
                        .build());
            }
        }
        reqObs.onCompleted();

        int tries = 0;
        while (rid[0] == null && tries++ < 100) Thread.sleep(100);
        return rid[0] == null ? "TIMEOUT" : rid[0];
    }

    /**
     * Extrai o ficheiro sample.png do resources para /tmp/sample-demo.png
     */
    private static Path extractSampleImage() throws IOException {
        try (InputStream in = Main.class.getResourceAsStream("/sample.png")) {
            if (in == null) {
                throw new FileNotFoundException("Resource sample.png não encontrado dentro do JAR.");
            }
            Path tmp = Files.createTempFile("sample-demo", ".png");
            Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
            tmp.toFile().deleteOnExit();
            return tmp;
        }
    }

    static void debug(String msg) {
        if (DEBUG) System.out.println(msg);
    }
}
