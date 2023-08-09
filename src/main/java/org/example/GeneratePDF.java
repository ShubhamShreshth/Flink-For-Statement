package org.example;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.ElementHandle;
import com.microsoft.playwright.Frame;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.options.AriaRole;
import com.microsoft.playwright.options.SelectOption;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;


public class GeneratePDF {

    public static void main(String[] args) throws FileNotFoundException{

        // memory used before the execution of the code
        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();

        // Local variable assignment for outside access
        int id = 0, transactionid = 0;
        double total_amount_due = 1.00, amount = 1.00;
        String date = "1", description = "1", name = "1", usersData = "0";

        // programs start-time
        long totalStartTime = System.currentTimeMillis();

        // Defining the path of the HTML
        String localFilePath = "/Users/se/Documents/Programming/Flink/temp/src/main/resources/statement.html";

        // Playwright assignment
        Playwright playwright = Playwright.create();
        // Launching the browser
        Browser browser = playwright.chromium().launch();
        // calling new browser page using playwright
        Page page = browser.newPage();

        // Defining the path of json file
        File jsonFile = new File("/Users/se/Documents/Programming/Flink/temp/src/main/resources/data.json");
        FileInputStream json_data = null;
        JsonNode jsonNode = null;
        
        try {

            // Trying to access the file using ObjectMapper of Jackson library
            json_data = new FileInputStream(jsonFile);
            ObjectMapper objectMapper = new ObjectMapper();
            jsonNode = objectMapper.readTree(json_data);

            // Accessing the json file
            JsonNode statementsArray = jsonNode.get("statements");
            if (statementsArray.isArray()) {
                for (JsonNode statementNode : statementsArray) {

                    // Duration to execute for single user
                    long startTime = System.currentTimeMillis();

                    // assignment of variables of json to local variables
                    id = statementNode.get("id").asInt();
                    name = statementNode.get("name").asText();
                    total_amount_due = statementNode.get("total_amount_due").asDouble();

                    // navigating to the HTML file
                    page.navigate("file://" + localFilePath);

                    // Created a variable userData to store all the transactions of that user
                    usersData = "[\n" +
                    "{\n" +
                    "    \"name\": \""+name+"\",\n" +
                    "    \"totalAmountDue\": "+total_amount_due+",\n" +
                    "    \"transactions\": [\n";

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

                    // pdf conversion
                    page.pdf(new Page.PdfOptions().setPath(Paths.get("/Users/se/Documents/Programming/Flink/temp/src/main/pdfs/"+name+".pdf")));
                    
                    // End time
                    long endTime = System.currentTimeMillis();
                    System.out.println("Time Taken for "+name+" in milliseconds: "+(endTime-startTime));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (json_data != null) {
                try {
                    json_data.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // closing the page to avoid memory leaks
        page.close();
        // closing browser and playwright to avoid un-neccesary usage of memory
        browser.close();
        playwright.close();

        // programs end-time
        long totalEndTime = System.currentTimeMillis();
        System.out.println("Total Time taken in milliseconds: "+(totalEndTime-totalStartTime));

        // memory used after code execution
        long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        System.out.println("Memory used in bytes: "+(afterUsedMem-beforeUsedMem));
    }
}
