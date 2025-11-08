package tpa1.worker;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

/**
 * ImageProcessorApp
 * Responsável por redimensionar imagens para a percentagem indicada.
 * Mantém o formato original (PNG ou JPG).
 */
public class Main {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("""
                Uso incorreto.
                Correto:
                  java -jar ImageProcessorApp.jar <imagem_entrada> <imagem_saida> <percentagem_redimensionamento>
                Exemplo:
                  java -jar ImageProcessorApp.jar foto.png reduzida.png 60
                """);
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        double pct;

        try {
            pct = Double.parseDouble(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("[Worker] Erro: percentagem inválida '" + args[2] + "'.");
            return;
        }

        // Ajustar caso seja percentagem 60 em vez de 0.6
        if (pct > 1) pct = pct / 100.0;
        if (pct <= 0 || pct > 1) {
            System.err.println("[Worker] Percentagem fora do intervalo válido (0 < p ≤ 1).");
            return;
        }

        System.out.printf("[Worker] Redimensionando imagem '%s' para %.0f%% → '%s'%n",
                inputPath, pct * 100, outputPath);

        resizeImage(inputPath, outputPath, pct);

        System.out.println("[Worker] Redimensionamento concluído com sucesso: " + outputPath);
    }

    /**
     * Redimensiona a imagem mantendo o formato original (PNG ou JPG).
     */
    public static void resizeImage(String inputPath, String outputPath, double percentage) {
        try {
            File inputFile = new File(inputPath);
            if (!inputFile.exists()) {
                System.err.println("[Worker] Erro: ficheiro de entrada não encontrado: " + inputPath);
                return;
            }

            BufferedImage originalImage = ImageIO.read(inputFile);
            if (originalImage == null) {
                System.err.println("[Worker] Erro: ficheiro não é uma imagem válida: " + inputPath);
                return;
            }

            int newWidth = Math.max(1, (int) (originalImage.getWidth() * percentage));
            int newHeight = Math.max(1, (int) (originalImage.getHeight() * percentage));

            BufferedImage resizedImg = new BufferedImage(newWidth, newHeight, BufferedImage.TYPE_INT_RGB);
            Graphics2D g2d = resizedImg.createGraphics();
            g2d.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC);
            g2d.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
            g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            g2d.drawImage(originalImage, 0, 0, newWidth, newHeight, null);
            g2d.dispose();

            // Detectar extensão de saída
            String format = detectFormat(outputPath);
            if (format == null) {
                System.err.println("[Worker] Extensão de saída desconhecida (use .png ou .jpg): " + outputPath);
                return;
            }

            ImageIO.write(resizedImg, format, new File(outputPath));

        } catch (IOException e) {
            System.err.println("[Worker] Erro ao processar imagem: " + e.getMessage());
        }
    }

    /**
     * Detecta o formato da imagem a partir da extensão do ficheiro.
     */
    private static String detectFormat(String path) {
        String lower = path.toLowerCase();
        if (lower.endsWith(".png")) return "png";
        if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) return "jpg";
        return null;
    }
}
