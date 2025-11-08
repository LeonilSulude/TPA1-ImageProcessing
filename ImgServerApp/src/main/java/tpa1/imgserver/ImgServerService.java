package tpa1.imgserver;

import io.grpc.stub.StreamObserver;
import img_client.ImgServerClientServiceGrpc;
import img_client.ImgClient.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Serviço gRPC do ImgServer:
 *  - Recebe uploads, valida e processa via Docker.
 *  - Disponibiliza downloads (ou redirecta) conforme estado no Redis.
 */
public class ImgServerService extends ImgServerClientServiceGrpc.ImgServerClientServiceImplBase {

    private final String selfIp;
    private final int selfPort;
    private final Path inputDir;
    private final Path outputDir;
    private final DockerLauncher docker;
    private final StateStore store;

    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("debug", "false"));
    private static void dbg(String msg) { if (DEBUG) System.out.println("[DEBUG] " + msg); }
    private static void log(String tag, String msg) { System.out.printf("[%s] %s%n", tag, msg); }

    public ImgServerService(String ip, int port, Path inputDir, Path outputDir,
                            DockerLauncher docker, StateStore store) {
        this.selfIp = ip;
        this.selfPort = port;
        this.inputDir = inputDir;
        this.outputDir = outputDir;
        this.docker = docker;
        this.store = store;
    }

    // =====================================================================================
    // Upload (stream de entrada)
    // =====================================================================================
    @Override
    public StreamObserver<ImageChunk> uploadImage(StreamObserver<UploadImageResponse> respObs) {
        String requestId = UUID.randomUUID().toString();
        final String[] ext = {null}; // extensão dinâmica (.png ou .jpg)
        final Path[] targetFile = {null};

        log("ImgServer", "Upload iniciado (requestId=" + requestId + ")");

        try {
            ByteArrayOutputStream headSniff = new ByteArrayOutputStream(16);
            final boolean[] headerChecked = {false};
            final boolean[] accepted = {true};
            final OutputStream[] out = {null};

            return new StreamObserver<>() {
                @Override
                public void onNext(ImageChunk chunk) {
                    if (!accepted[0]) return;
                    byte[] data = chunk.getData().toByteArray();

                    try {
                        // Validar formato logo no início
                        if (!headerChecked[0]) {
                            int toCopy = Math.min(data.length, 16 - headSniff.size());
                            if (toCopy > 0) headSniff.write(data, 0, toCopy);

                            if (headSniff.size() >= 8) {
                                headerChecked[0] = true;
                                ext[0] = detectImageExtension(headSniff.toByteArray());
                                if (ext[0] == null) {
                                    accepted[0] = false;
                                    log("ImgServer", "Upload rejeitado (formato inválido, requestId=" + requestId + ")");
                                    return;
                                }

                                // cria o ficheiro com a extensão correta
                                targetFile[0] = inputDir.resolve("in-" + requestId + ext[0]);
                                out[0] = Files.newOutputStream(targetFile[0],
                                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                                dbg("Criado ficheiro de upload: " + targetFile[0]);
                            }
                        }

                        if (accepted[0] && out[0] != null) {
                            out[0].write(data);
                        }

                    } catch (IOException e) {
                        accepted[0] = false;
                        dbg("IOException no upload: " + e.getMessage());
                    }
                }

                @Override
                public void onError(Throwable t) {
                    try { if (out[0] != null) out[0].close(); } catch (IOException ignored) {}
                    log("ImgServer", "Upload falhou (" + requestId + "): " + t.getMessage());
                    safeDelete(targetFile[0]);
                }

                @Override
                public void onCompleted() {
                    try { if (out[0] != null) out[0].close(); } catch (IOException ignored) {}

                    if (!accepted[0] || ext[0] == null) {
                        safeDelete(targetFile[0]);
                        UploadImageResponse resp = UploadImageResponse.newBuilder()
                                .setRequestId("ERROR")
                                .setMessage("Formato inválido (só PNG/JPEG)")
                                .build();
                        respObs.onNext(resp);
                        respObs.onCompleted();
                        return;
                    }

                    // ======= salvar estado PROCESSING no Redis =======
                    String inName = "in-" + requestId + ext[0];
                    String outName = "out-" + requestId + ext[0];

                    Map<String, Object> meta = new HashMap<>();
                    meta.put("requestId", requestId);
                    meta.put("status", "PROCESSING");
                    meta.put("imgServerIP", selfIp);
                    meta.put("imgServerPort", selfPort);
                    meta.put("inputFile", "/images/input/" + inName);
                    meta.put("outputFile", "/images/output/" + outName);
                    store.put(requestId, meta);

                    log("ImgServer", "Imagem recebida e marcada como PROCESSING (" + requestId + ")");

                    // ======= lançar container Docker =======
                    String workerImage = System.getProperty("workerImage", "imageprocessorapp");
                    double pct = Double.parseDouble(System.getProperty("resizePct", "0.5"));
                    String containerId = docker.launchResize(workerImage,
                            inputDir.getParent().toString(), inName, outName, pct);
                    meta.put("containerId", containerId);
                    store.put(requestId, meta);
                    log("ImgServer", "Container Docker iniciado (id=" + containerId + ", requestId=" + requestId + ")");

                    // resposta imediata ao cliente
                    UploadImageResponse resp = UploadImageResponse.newBuilder()
                            .setRequestId(requestId)
                            .setMessage("RECEIVED")
                            .build();
                    respObs.onNext(resp);
                    respObs.onCompleted();

                    // ======= thread para monitorizar conclusão =======
                    new Thread(() -> {
                        try {
                            while (docker.isRunning(containerId)) Thread.sleep(500);
                            Map<String, Object> done = store.get(requestId);
                            if (done != null) {
                                done.put("status", "DONE");
                                store.put(requestId, done);
                                log("ImgServer", "Processamento concluído e marcado como DONE (" + requestId + ")");
                            }
                        } catch (InterruptedException ignored) {
                        } finally {
                            docker.remove(containerId);
                        }
                    }, "worker-wait-" + requestId).start();
                }
            };

        } catch (Exception e) {
            UploadImageResponse resp = UploadImageResponse.newBuilder()
                    .setRequestId("ERROR")
                    .setMessage("Falha ao iniciar upload: " + e.getMessage())
                    .build();
            respObs.onNext(resp);
            respObs.onCompleted();
            return new StreamObserver<>() {
                public void onNext(ImageChunk v) {}
                public void onError(Throwable t) {}
                public void onCompleted() {}
            };
        }
    }

    // =====================================================================================
    // Download (stream de saída)
    // =====================================================================================
    @Override
    public void downloadImage(DownloadImageRequest request, StreamObserver<DownloadImageChunk> resp) {
        String requestId = request.getRequestId();
        Map<String, Object> meta = store.get(requestId);

        if (meta == null) {
            log("ImgServer", "Pedido de download rejeitado — requestId desconhecido (" + requestId + ")");
            sendStatus(resp, "ERROR", null, 0, "requestId desconhecido");
            return;
        }

        String status = String.valueOf(meta.get("status"));
        String ownerIp = String.valueOf(meta.get("imgServerIP"));
        int ownerPort = Integer.parseInt(String.valueOf(meta.get("imgServerPort")));
        String outPath = String.valueOf(meta.get("outputFile"));

        // redirect se o ficheiro não é deste ImgServer
        if (!ownerIp.equals(selfIp) || ownerPort != selfPort) {
            log("ImgServer", "Pedido redirecionado para " + ownerIp + ":" + ownerPort + " (requestId=" + requestId + ")");
            sendStatus(resp, "REDIRECT", ownerIp, ownerPort, "Mover para o servidor que possui o ficheiro");
            return;
        }

        // ainda em processamento
        if (!"DONE".equals(status)) {
            log("ImgServer", "Pedido de download ainda em processamento (" + requestId + ")");
            sendStatus(resp, status, null, 0, "Ainda não pronto");
            return;
        }

        // DONE e local → stream do ficheiro
        Path realOut = outputDir.resolve(Paths.get(outPath).getFileName());
        if (!Files.exists(realOut)) {
            log("ImgServer", "Erro: ficheiro final não encontrado (" + realOut + ")");
            sendStatus(resp, "ERROR", null, 0, "Ficheiro não encontrado no volume");
            return;
        }

        log("ImgServer", "Download iniciado (" + requestId + ")");
        try (InputStream in = Files.newInputStream(realOut)) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) != -1) {
                resp.onNext(DownloadImageChunk.newBuilder()
                        .setData(com.google.protobuf.ByteString.copyFrom(buf, 0, n))
                        .build());
            }
            sendStatus(resp, "DONE", null, 0, "OK");
            log("ImgServer", "Download concluído com sucesso (" + requestId + ")");
        } catch (IOException e) {
            sendStatus(resp, "ERROR", null, 0, e.getMessage());
        }
    }

    // =====================================================================================
    // Helpers
    // =====================================================================================
    private static void sendStatus(StreamObserver<DownloadImageChunk> resp, String st, String ip, int port, String msg) {
        DownloadImageStatus.Builder b = DownloadImageStatus.newBuilder()
                .setStatus(st)
                .setMessage(msg == null ? "" : msg);
        if ("REDIRECT".equals(st)) b.setRedirectIp(ip).setRedirectPort(port);
        resp.onNext(DownloadImageChunk.newBuilder().setStatus(b.build()).build());
        resp.onCompleted();
    }

    private static String detectImageExtension(byte[] head) {
        if (head == null || head.length < 4) return null;

        // PNG
        byte[] PNG = {(byte) 0x89, 0x50, 0x4E, 0x47};
        boolean png = true;
        for (int i = 0; i < PNG.length; i++) {
            if (head[i] != PNG[i]) { png = false; break; }
        }
        if (png) return ".png";

        // JPEG
        if (head[0] == (byte) 0xFF && head[1] == (byte) 0xD8) return ".jpg";

        return null;
    }

    private static void safeDelete(Path p) {
        try { if (p != null) Files.deleteIfExists(p); } catch (IOException ignored) {}
    }
}
