package tpa1.client;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import manager_client.*;
import manager_client.ManagerClient.*;
import img_client.ImgClient.*;
import img_client.ImgServerClientServiceGrpc;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class Main {
    static boolean DEBUG = Boolean.parseBoolean(System.getProperty("debug", "false"));
    static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) throws Exception {

        // === Configurações básicas de rede (IPv4 + DNS) ===
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("io.grpc.internal.DnsNameResolverProvider.enable_jndi", "false");
        System.setProperty("io.grpc.internal.DnsNameResolverProvider.enable_service_config", "false");

        // === Reforçar load balancer padrão pick_first ===
        io.grpc.LoadBalancerRegistry.getDefaultRegistry()
                .register(new io.grpc.internal.PickFirstLoadBalancerProvider());
        System.out.println("[DEBUG] PickFirstLoadBalancerProvider registado manualmente.");

        // === Forçar DNS resolver (sem reflection, mais seguro) ===
        io.grpc.NameResolverRegistry.getDefaultRegistry()
                .register(new io.grpc.internal.DnsNameResolverProvider());
        System.out.println("[DEBUG] DnsNameResolverProvider registado manualmente.");

        debug("[DEBUG] Stack IPv4 + DNS configurado com sucesso.");

        // === Modo direto (salta Manager) OU modo normal ===
        String directIpProp = System.getProperty("imgIp");
        String directPortProp = System.getProperty("imgPort");

        String imgIp;
        int imgPort;

        if (directIpProp != null && directPortProp != null) {
            // ---------- MODO DIRETO ----------
            imgIp = directIpProp.trim();
            imgPort = Integer.parseInt(directPortProp.trim());
            System.out.printf("[Client] MODO DIRETO: a usar ImgServer %s:%d (sem contactar Manager)%n", imgIp, imgPort);
        } else {
            // ---------- MODO NORMAL (usa Manager) ----------
            // === Obter ImgServer do ManagerServer ===
            String managerIp = System.getProperty("managerIp", "127.0.0.1");
            int managerPort = Integer.parseInt(System.getProperty("managerPort", "50051"));
            System.out.printf("[Client] Ligando ao ManagerServer %s:%d ...%n", managerIp, managerPort);


            ManagedChannel managerChannel = NettyChannelBuilder.forAddress(managerIp, managerPort)
                    .usePlaintext()
                    .build();

            var mstub = ManagerServerClientServiceGrpc.newBlockingStub(managerChannel);
            GetImgServerResponse sel = mstub.getImgServer(GetImgServerRequest.getDefaultInstance());
            managerChannel.shutdownNow();

            imgIp = sel.getImgServerIp();
            imgPort = sel.getImgServerPort();

            System.out.printf("[Client] ImgServer designado: %s:%d%n", imgIp, imgPort);

        }

        // === Canal principal persistente ===
        ManagedChannel mainChannel = NettyChannelBuilder.forAddress(imgIp, imgPort)
                .usePlaintext()
                .build();
        ImgServerClientServiceGrpc.ImgServerClientServiceStub asyncStub =
                ImgServerClientServiceGrpc.newStub(mainChannel);

        // === Menu interativo ===
        while (true) {
            System.out.println("\n=== Cliente ImgServer ===");
            System.out.println("1. Enviar imagens");
            System.out.println("2. Descarregar imagens");
            System.out.println("3. Sair");
            System.out.print("Escolha uma das opções: ");
            String op = scanner.nextLine().trim();

            switch (op) {
                case "1" -> enviarImagens(asyncStub);
                case "2" -> descarregarImagens(asyncStub);
                case "3" -> {
                    System.out.println("[Client] Encerrando aplicação...");
                    mainChannel.shutdownNow();
                    return;
                }
                default -> System.out.println("[WARN] Opção inválida!");
            }
        }
    }

    // === Envio de múltiplas imagens ===
    static void enviarImagens(ImgServerClientServiceGrpc.ImgServerClientServiceStub stub) throws Exception {
        List<String> files = new ArrayList<>();
        System.out.println("Digite os caminhos das imagens a enviar (ou 'exit' para terminar):");
        while (true) {
            System.out.print("> ");
            String file = scanner.nextLine().trim();
            if (file.equalsIgnoreCase("exit") || file.isEmpty()) break;
            if (!Files.exists(Path.of(file))) {
                System.out.println("[WARN] Ficheiro não encontrado: " + file);
                continue;
            }
            String lower = file.toLowerCase();
            if (!(lower.endsWith(".png") || lower.endsWith(".jpg") || lower.endsWith(".jpeg"))) {
                System.out.println("[WARN] Ignorado (não é imagem suportada): " + file);
                continue;
            }
            files.add(file);
        }

        if (files.isEmpty()) {
            System.out.println("[INFO] Nenhuma imagem válida para enviar.");
            return;
        }

        for (String f : files) {
            String req = upload(stub, f);
            System.out.printf("[Client] Enviado %s -> requestId = %s%n", f, req);
        }
    }

    // === Upload simples ===
    static String upload(ImgServerClientServiceGrpc.ImgServerClientServiceStub stub, String file) throws Exception {
        final String[] rid = {null};
        StreamObserver<UploadImageResponse> respObs = new StreamObserver<>() {
            public void onNext(UploadImageResponse v) {
                rid[0] = v.getRequestId();
                System.out.println("[Client] requestId=" + rid[0]);
            }
            public void onError(Throwable t) {
                System.err.println("[Client] Erro no upload: " + t.getMessage());
            }
            public void onCompleted() {}
        };

        StreamObserver<ImageChunk> reqObs = stub.uploadImage(respObs);
        try (InputStream in = Files.newInputStream(Path.of(file))) {
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
        while (rid[0] == null && tries++ < 50) Thread.sleep(100);
        return rid[0];
    }

    // === Download de várias imagens (assíncrono) ===
    static void descarregarImagens(ImgServerClientServiceGrpc.ImgServerClientServiceStub stub) {
        List<String> ids = new ArrayList<>();
        System.out.println("Digite os requestIds a descarregar (ou 'exit' para terminar):");
        while (true) {
            System.out.print("> ");
            String id = scanner.nextLine().trim();
            if (id.equalsIgnoreCase("exit") || id.isEmpty()) break;
            ids.add(id);
        }

        for (String reqId : ids) {
            downloadAsync(stub, reqId);
        }
    }

    // === Download streaming com suporte a redirect ===
    static void downloadAsync(ImgServerClientServiceGrpc.ImgServerClientServiceStub stub, String requestId) {

        StreamObserver<DownloadImageChunk> respObs = new StreamObserver<>() {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();

            @Override
            public void onNext(DownloadImageChunk chunk) {
                if (chunk.hasData()) {
                    try {
                        buffer.write(chunk.getData().toByteArray());
                    } catch (IOException e) {
                        System.err.println("[Client] Erro ao escrever chunk: " + e.getMessage());
                    }
                } else if (chunk.hasStatus()) {
                    DownloadImageStatus s = chunk.getStatus();
                    switch (s.getStatus()) {
                        case "REDIRECT" -> {
                            System.out.printf("[Client] Redirecionado para %s:%d%n", s.getRedirectIp(), s.getRedirectPort());
                            ManagedChannel redirectChannel = NettyChannelBuilder.forAddress(s.getRedirectIp(), s.getRedirectPort())
                                    .usePlaintext().build();
                            var redirectStub = ImgServerClientServiceGrpc.newStub(redirectChannel);
                            downloadWithChannelClose(redirectStub, requestId, redirectChannel);
                        }
                        case "PROCESSING" -> {
                            System.out.println("[Client] Ainda a processar " + requestId + " ...");
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException ignored) {}
                            stub.downloadImage(DownloadImageRequest.newBuilder()
                                    .setRequestId(requestId).build(), this);
                        }
                        case "DONE" -> {
                            String ext = guessImageExtension(buffer.toByteArray());
                            String name = "download-" + requestId + ext;
                            try {
                                Files.write(Path.of(name), buffer.toByteArray());
                                System.out.println("[Client] Download concluído: " + name);
                            } catch (IOException e) {
                                System.err.println("[Client] Erro ao salvar ficheiro: " + e.getMessage());
                            }
                        }
                        case "ERROR" -> System.err.println("[Client] Erro: " + s.getMessage());
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("[Client] Erro no streaming: " + t.getMessage());
            }

            @Override
            public void onCompleted() {}
        };

        stub.downloadImage(DownloadImageRequest.newBuilder().setRequestId(requestId).build(), respObs);
    }

    static void downloadWithChannelClose(
            ImgServerClientServiceGrpc.ImgServerClientServiceStub stub,
            String requestId,
            ManagedChannel channelToClose) {

        stub.downloadImage(
                DownloadImageRequest.newBuilder().setRequestId(requestId).build(),
                new StreamObserver<DownloadImageChunk>() {

                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();

                    @Override
                    public void onNext(DownloadImageChunk chunk) {
                        // Igual ao onNext normal, processa dados/status...
                    }

                    @Override
                    public void onError(Throwable t) {
                        System.err.println("[Client] Erro em redirect: " + t.getMessage());
                        channelToClose.shutdownNow();  // FECHA AQUI
                    }

                    @Override
                    public void onCompleted() {
                        System.out.println("[Client] Download redirect completo!");
                        channelToClose.shutdownNow();  // ← FECHA AQUI QUANDO TERMINA
                    }
                }
        );
    }


    static void debug(String msg) {
        if (DEBUG) System.out.println(msg);
    }

    // === Deduz extensão pela assinatura mágica ===
    static String guessImageExtension(byte[] data) {
        if (data.length > 4 && data[0] == (byte) 0x89 && data[1] == 0x50 && data[2] == 0x4E && data[3] == 0x47)
            return ".png";
        if (data.length > 2 && data[0] == (byte) 0xFF && data[1] == (byte) 0xD8)
            return ".jpg";
        return ".img";
    }
}
