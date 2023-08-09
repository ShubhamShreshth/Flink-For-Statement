package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.interactive.form.PDAcroForm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class StatementPDFGenerator {

    private static final Logger logger = Logger.getLogger(StatementPDFGenerator.class.getName());
    
    // set the minio access point and bucket names
    static final String bucket = "bucket-for-statement";
    static final String prefix = "";
    static final String objectKey = "statement.html";  // object key is name of the file
    static final String resultbucket = "statement-result";
    static final String contentType = "application/pdf";

    static final String minioendpoint = "http://172.25.104.22:9000"; // Replace with your MinIO endpoint
    // static final String minioEndpoint = "http://localhost:9000"; // Replace with your MinIO endpoint
    // static final String minioEndpoint = "http://127.0.0.1:9000"; // Replace with your MinIO endpoint
    // static final String minioEndpoint = "http://0.0.0.0:9090"; // Replace with your MinIO endpoint
    static final String awsRegion = "us-east-1"; // Replace with your desired region code

    // Set the MinIO access key and secret key
    static final String accessKey = "mWVur5X8nJYogPb534m1"; // Replace with your MinIO access key
    static final String secretKey = "nGamsK0L986CPWYAwElrmePr01otemIwS4grz3mu"; // Replace with your MinIO secret key
   
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read data from S3 files
        DataStream<String> inputData = env.fromCollection(listS3Files(bucket,prefix))
            .flatMap(new S3FileReader());

        // Process the input data and generate PDFs
        DataStream<Tuple2<String, String>> pdfData = inputData.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {

                // defining variables
                int id = 0, transactionid = 0;
                double total_amount_due = 1.00, amount = 1.00;
                String date = "1", description = "1", name = "1", usersData = "0";

                // Playwright assignment
                Playwright playwright = Playwright.create();
                Browser browser = playwright.chromium().launch();
                Page page = browser.newPage();

                // Defining the path of the HTML stored i minio
                // String localHTMLPath = "/src/main/resources/statement.html";
                String HTMLPath = minioendpoint + "/" + bucket + "/" + objectKey;  // standard syntax to access the files of minio usig url

                // Defining the path of json file
                // File jsonFile = new File("bucket-for-statement/data.json");
                // FileInputStream json_data = null;
                JsonNode jsonNode = null;

                try {

                    // Trying to access the file using ObjectMapper of Jackson library
                    // json_data = new FileInputStream(jsonFile);
                    ObjectMapper objectMapper = new ObjectMapper();
                    jsonNode = objectMapper.readTree(value);

                    // Accessing the json file for every user
                    JsonNode statementsArray = jsonNode.get("statements");
                    if (statementsArray.isArray()) {
                        for (JsonNode statementNode : statementsArray) {

                            // assignment of variables of json to local variables
                            id = statementNode.get("id").asInt();
                            name = statementNode.get("name").asText();
                            total_amount_due = statementNode.get("total_amount_due").asDouble();

                            // navigating to the HTML file
                            // page.navigate("file://" + localHTMLPath);
                            page.navigate(HTMLPath);

                            // Created a variable userData to store all the transactions data of that user
                            usersData = "[\n" +
                            "{\n" +
                            "    \"name\": \""+name+"\",\n" +
                            "    \"totalAmountDue\": "+total_amount_due+",\n" +
                            "    \"transactions\": [\n";

                            // iterating through every transactions of each user
                            JsonNode transactionsArray = statementNode.get("transactions");
                            if (transactionsArray.isArray()) {
                                for (JsonNode transactionNode : transactionsArray) {

                                    // assigning the values of json to HTML ids using evaluate function of page in playwright
                                    transactionid = transactionNode.get("Tid").asInt();
                                    date = transactionNode.get("date").asText();
                                    description = transactionNode.get("description").asText();
                                    amount = transactionNode.get("amount").asDouble();

                                    // adding transaction to that variable
                                    String tempData = "        { \"date\": \""+date+"\", \"description\": \""+description+"\", \"amount\": "+amount+" },\n";
                                    usersData = usersData + tempData;
                                }
                            }

                            usersData = usersData + 
                            "    ]\n" +
                            "},\n" +
                            "]";

                            // adding json data into HTML
                            // passing javascript code to make HTML dynamic, example when transactions increases it will also increase
                            page.evaluate("const usersData = " + usersData + ";" +
                            "const userTemplate = document.getElementById('user-template');" +
                            "const usersContainer = document.createElement('div');" +
                            "usersData.forEach(user => {" +
                            "const userDiv = userTemplate.cloneNode(true);" +
                            "userDiv.removeAttribute('id');" +
                            "userDiv.style.display = 'block';" +
                            "userDiv.querySelector('.user-name').textContent = user.name;" +
                            "userDiv.querySelector('.total-amount-value').textContent = '$' + user.totalAmountDue.toFixed(2);" +
                            "const transactionsTable = userDiv.querySelector('table');" +
                            "user.transactions.forEach(transaction => {" +
                            "const row = document.createElement('tr');" +
                            "row.innerHTML = `<td>${transaction.date}</td><td>${transaction.description}</td><td>$${transaction.amount.toFixed(2)}</td>`;" +
                            "transactionsTable.appendChild(row);" +
                            "});" +
                            "usersContainer.appendChild(userDiv);" +
                            "});" +
                            "document.body.appendChild(usersContainer);");

                            // Generate a PDF for the invoice and storing locally in pdfs folder inside main
                            page.pdf(new Page.PdfOptions().setPath(Paths.get("src/main/pdfs/"+name+".pdf")));
                            String pdfpath = "src/main/pdfs/"+name+".pdf";

                            // uploading pdf to minio bucket
                            S3Client s3Client = createS3Client();
                            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                                .bucket(resultbucket)
                                .key(name+".pdf")
                                .contentType(contentType)
                                .build();
                            s3Client.putObject(putObjectRequest, Paths.get(pdfpath));
                            System.out.println("\n\n\n\nPDF file uploaded to MinIO: " + objectKey+"\n\n\n\n");

                            // deleting files from local file system/storage
                            try {
                                // Delete the file
                                Files.delete(Paths.get(pdfpath));
                                System.out.println("\n\n\n\nFile deleted: " + pdfpath+"\n\n\n\n");
                            } catch (IOException e) {
                                System.err.println("\n\n\n\nError deleting file: " + e.getMessage()+"\n\n\n\n");
                            }
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // mandatory return statement of the function
                return Tuple2.of(name, String.valueOf(total_amount_due));
            }
        });

        // Execute the Flink job
        env.execute("Statement PDF Generator");
        env.close();
    }

    // Reading the S3 files
    private static class S3FileReader implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String file, Collector<String> out) throws Exception {

            S3Client s3Client = createS3Client();

            // Retrieve the content of the S3 file
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(file)
                    .build();

            // Create a ConsoleHandler to output logs to the console
            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(Level.ALL);
            logger.addHandler(consoleHandler);

            // retrieve content type of files from minio
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
            .bucket(bucket)
            .key(file)
            .build();

            HeadObjectResponse headObjectResponse = s3Client.headObject(headObjectRequest);
            String contentType = headObjectResponse.contentType();

            // check point to only read json file
            if ("application/json".equals(contentType)){
                try (InputStream inputStream = s3Client.getObject(getObjectRequest)) {
                    // Process the content as a string
                    String fileContent = readInputStreamAsString(inputStream);
                    if (fileContent!=null){
                        
                        logger.info("\n\n\n\nContents of files are: "+fileContent+"\n\n\n\n");

                        String[] rows = fileContent.split("\n");

                        // Emit each row as a separate string
                        for (String row : rows) {
                            out.collect(row);
                        }
                    }
                    else{logger.info("Error reading S3 files. No content in the files");}
                } catch (IOException e) {
                    e.printStackTrace();
                    logger.info("Error reading S3 files. No content in the files. It gives error: "+e);
                }

            }
        }
    }
    
    private static String readInputStreamAsString(InputStream inputStream) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        return result.toString(StandardCharsets.UTF_8.name());
    }

    public static List<String> listS3Files(String bucket, String prefix){

        S3Client s3Client = createS3Client();
        ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build();
        ListObjectsV2Iterable response = s3Client.listObjectsV2Paginator(request);
        List<String> list = new ArrayList<>();
        for (ListObjectsV2Response page : response) {
            page.contents().forEach((S3Object object) -> {
                list.add(object.key());
            });
        }
        return list;
    }

    static S3Client createS3Client(){

        // Create AWS credentials for MinIO
        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);

        return  S3Client.builder()
            .endpointOverride(URI.create(minioendpoint))
            .region(Region.of(awsRegion))
            .credentialsProvider(() -> credentials)
            .build();

    }
}
