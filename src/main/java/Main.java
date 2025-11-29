import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class Main implements RequestHandler<S3Event, String> {
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private static String DESTINATION_BUCKET = "bucket-client-vw";
    String destino = "";

    public static void main(String[] args) {

    }

    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        // arquivo que chegou
        String sourceBucket = s3Event.getRecords().get(0).getS3().getBucket().getName();
        String sourceKey = s3Event.getRecords().get(0).getS3().getObject().getKey();

        context.getLogger().log("Processando arquivo: " + sourceBucket + "/" + sourceKey);

        try {
            // 1. pegar JSON do bucket de origem
            S3Object s3Object = s3Client.getObject(sourceBucket, sourceKey);

            if(sourceKey.contains("imagens")){
                context.getLogger().log("Entrei no if que contém imagens");

                destino = "suporte/imagens/" + sourceKey;

                // 4. enviar pro bucket client
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentType("application/json");

                context.getLogger().log("Enviando json imagens bucket client");

                s3Client.putObject(
                        DESTINATION_BUCKET,
                        destino,
                        s3Object.getObjectContent(),
                        metadata
                );
            } else if (sourceKey.contains("processos")) {
                context.getLogger().log("Entrei no if que contém processos");

                destino = "suporte/micro/" + sourceKey;

                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentType("application/json");

                s3Client.putObject(
                        DESTINATION_BUCKET,
                        destino,
                        s3Object.getObjectContent(),
                        metadata
                );

            } else if (sourceKey.contains("principal")) {
                context.getLogger().log("Processando JSON de REDE (arquivo principal)...");

                Path origemTmp = Paths.get("/tmp/saida.json");

                Files.copy(
                        s3Object.getObjectContent(),
                        origemTmp,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING
                );

                context.getLogger().log("Arquivo bruto salvo em /tmp/saida.json");

                String jsonStr = Files.readString(origemTmp);
                JSONArray jsonArrayOriginal = new JSONArray(jsonStr);

                JSONObject ultimo = jsonArrayOriginal.getJSONObject(jsonArrayOriginal.length() - 1);

                List<String> camposRede = List.of(
                        "Net_Down_(Mbps)",
                        "Net_Up_(Mbps)",
                        "Pacotes_IN_(intervalo)",
                        "Pacotes_OUT_(intervalo)",
                        "Perda_de_Pacotes_(%)",
                        "Conexões_TCP_ESTABLISHED",
                        "Latencia_(ms)"
                );

                JSONObject filtrado = new JSONObject();
                filtrado.put("Nome_da_Maquina", ultimo.get("Nome_da_Maquina"));
                filtrado.put("Data_da_Coleta", ultimo.get("Data_da_Coleta"));

                for (String campo : camposRede) {
                    if (ultimo.has(campo)) {
                        filtrado.put(campo, ultimo.get(campo));
                    }
                }

                // Nome final
                String nomeFinal = sourceKey.replace("_principal", "");
                if (!nomeFinal.endsWith(".json")) nomeFinal += ".json";

                Path destinoTmp = Paths.get("/tmp/" + nomeFinal);
                Files.writeString(destinoTmp, filtrado.toString(2));

                context.getLogger().log("JSON filtrado salvo em: /tmp/" + nomeFinal);

                String caminhoFinal = "suporte/rede/" + nomeFinal;

                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentType("application/json");
                metadata.setContentLength(Files.size(destinoTmp));

                s3Client.putObject(
                        DESTINATION_BUCKET,
                        caminhoFinal,
                        new FileInputStream(destinoTmp.toFile()),
                        metadata
                );

                context.getLogger().log("JSON de rede enviado para: " + caminhoFinal);

                // IMPLEMENTANDO DASHBOARD MACRO COMPONENTES
                // Processamento para Componentes (Dashboard Macro)
                List<String> camposComponentes = List.of(
                        "Uso_de_Cpu",
                        "Uso_de_RAM",
                        "Uso_de_Disco"
                );

                JSONObject filtradoComponentes = new JSONObject();
                filtradoComponentes.put("Nome_da_Maquina", ultimo.get("Nome_da_Maquina"));
                filtradoComponentes.put("Data_da_Coleta", ultimo.get("Data_da_Coleta"));

                for (String campo : camposComponentes) {
                    if (ultimo.has(campo)) {
                        filtradoComponentes.put(campo, ultimo.get(campo));
                    }
                }

                String nomeComponentes = sourceKey.replace("_principal", "_componentes");
                if (!nomeComponentes.endsWith(".json")) nomeComponentes += ".json";

                Path destinoComponentesTmp = Paths.get("/tmp/" + nomeComponentes);
                Files.writeString(destinoComponentesTmp, filtradoComponentes.toString(2));

                context.getLogger().log("JSON de componentes salvo em: /tmp/" + nomeComponentes);

                String caminhoComponentesFinal = "suporte/macro/componentes/" + nomeComponentes;

                ObjectMetadata metadataComponentes = new ObjectMetadata();
                metadataComponentes.setContentType("application/json");
                metadataComponentes.setContentLength(Files.size(destinoComponentesTmp));

                s3Client.putObject(
                        DESTINATION_BUCKET,
                        caminhoComponentesFinal,
                        new FileInputStream(destinoComponentesTmp.toFile()),
                        metadataComponentes
                );

                context.getLogger().log("JSON de componentes enviado para: " + caminhoComponentesFinal);

                // IMPLEMENTANDO DASHBOARD MICRO SUPORTE
                // Processamento para Componentes (Dashboard Macro)
                List<String> listaDadosSuporteMicro = List.of(
                        "Uso_de_RAM",
                        "Uso_de_Cpu",
                        "Uso_de_Disco",
                        "Uptime_(s)",
                        "Disco_usado_(bytes)",
                        "Disco_livre_(bytes)",
                        "Disco_total_(bytes)",
                        "RAM_total_(bytes)",
                        "RAM_usada_(bytes)",
                        "Nome_da_Maquina",
                        "Data_da_Coleta"
                );

                JSONObject dadosFiltrados = new JSONObject();
                dadosFiltrados.put("Nome_da_Maquina", ultimo.get("Nome_da_Maquina"));
                dadosFiltrados.put("Data_da_Coleta", ultimo.get("Data_da_Coleta"));

                for (String campo : listaDadosSuporteMicro) {
                    if (ultimo.has(campo)) {
                        dadosFiltrados.put(campo, ultimo.get(campo));
                    }
                }

                String nomeArquivo = sourceKey;
                if (!nomeArquivo.endsWith(".json")) nomeArquivo += ".json";

                Path destinoSuporteMicroTemp = Paths.get("/tmp/" + nomeArquivo);
                Files.writeString(destinoSuporteMicroTemp, dadosFiltrados.toString(2));

                context.getLogger().log("JSON de componentes salvo em: /tmp/" + nomeArquivo);

                String caminhoSuporteMicro = "suporte/micro/" + nomeArquivo;

                ObjectMetadata metadataSuporteMicro = new ObjectMetadata();
                metadataSuporteMicro.setContentType("application/json");
                metadataSuporteMicro.setContentLength(Files.size(destinoSuporteMicroTemp));

                s3Client.putObject(
                        DESTINATION_BUCKET,
                        caminhoSuporteMicro,
                        new FileInputStream(destinoSuporteMicroTemp.toFile()),
                        metadataSuporteMicro
                );

                context.getLogger().log("JSON de componentes enviado para: " + caminhoSuporteMicro);
            }
        else if (sourceKey.contains("previsoes")) {
                context.getLogger().log("Entrei no if que contém previsoes");

                destino = "analista/previsoes/" + sourceKey;

                // 4. enviar pro bucket client
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentType("application/json");

                context.getLogger().log("Enviando json previsoes bucket client");

                s3Client.putObject(
                        DESTINATION_BUCKET,
                        destino,
                        s3Object.getObjectContent(),
                        metadata
                );
            }


            return "Processado com sucesso: " + sourceKey;
        }

        catch (Exception ex) {
            context.getLogger().log("ERRO:" + ex.getMessage());
            return "Falha ao processar o arquivo.";
        }


    }

}
