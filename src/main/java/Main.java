import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.lambda.runtime.events.S3Event;

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
                DESTINATION_BUCKET = "bucket-client-vw/suporte/micro";
                //...
            }
            return "Processado com sucesso: " + sourceKey;
        }

        catch (Exception ex) {
            context.getLogger().log("ERRO:" + ex.getMessage());
            return "Falha ao processar o arquivo.";
        }


    }

}
