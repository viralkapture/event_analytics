package com.ingest.analyticevents.Schedular;

import com.ingest.analyticevents.Dto.EndpointDetail;
import com.ingest.analyticevents.service.ClickHouseService;
import com.ingest.analyticevents.service.SendGridEmailService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.io.IOException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class ClientApiAlertScheduler {

    private static final Logger logger = LoggerFactory.getLogger(ClientApiAlertScheduler.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Autowired
    private ClickHouseService clickHouseService;
    @Autowired
    private SendGridEmailService sendGridEmailService;

    private static Map<String, List<String>> DOMAIN_TO_RECIPIENTS;

    static {
        try {
            DOMAIN_TO_RECIPIENTS = new ObjectMapper().readValue(
                    new File("src/main/resources/domain-email-map.json"),
                    new TypeReference<Map<String, List<String>>>() {}
            );
        } catch (IOException e) {
            DOMAIN_TO_RECIPIENTS = Map.of("default", List.of("fallback@example.com"));
            LoggerFactory.getLogger(ClientApiAlertScheduler.class).error("Failed to load domain-email mapping JSON: {}", e.getMessage());
        }
    }

    //@Scheduled(fixedRate = 600000)
    public void checkApiEndpointsAndSendAlert() {
        try {
            Timestamp fromTime = Timestamp.valueOf(LocalDateTime.now().minusMinutes(10));
            String currentTime = LocalDateTime.now().format(DATE_FORMATTER);

            logger.info("Checking API endpoints from: {}", fromTime);

            List<EndpointDetail> slowEndpoints = clickHouseService.fetchSlowEndpointsDetailed(fromTime);
            List<EndpointDetail> errorEndpoints = clickHouseService.fetchErrorEndpointsDetailed(fromTime);

            if (slowEndpoints.isEmpty() && errorEndpoints.isEmpty()) {
                logger.info("No slow or error endpoints found in the last 10 minutes");
                return;
            }

            Map<String, List<EndpointDetail>> slowByDomain = clickHouseService.groupSlowDetailsByDomain(slowEndpoints);
            Map<String, List<EndpointDetail>> errorByDomain = clickHouseService.groupErrorDetailsByDomain(errorEndpoints);

            Set<String> allDomains = new HashSet<>();
            allDomains.addAll(slowByDomain.keySet());
            allDomains.addAll(errorByDomain.keySet());

            for (String domain : allDomains) {
                List<EndpointDetail> domainSlow = slowByDomain.getOrDefault(domain, List.of());
                List<EndpointDetail> domainError = errorByDomain.getOrDefault(domain, List.of());

                int slowCount = domainSlow.stream().mapToInt(EndpointDetail::getCount).sum();
                int errorCount = domainError.stream().mapToInt(EndpointDetail::getCount).sum();

                String subject = String.format("\uD83D\uDEA8 [%s] API Alert - %d Slow, %d Error", domain, slowCount, errorCount);
                String body = "These client APIs have failed. Please check the attached CSV file for details.";

                File csvFile = generateCsvForDomain(domain, domainSlow, domainError);

                List<String> recipients = DOMAIN_TO_RECIPIENTS.getOrDefault(domain, DOMAIN_TO_RECIPIENTS.get("default"));
                sendGridEmailService.sendEmailToMultipleRecipients(
                        String.join(",", recipients),
                        subject,
                        body,
                        List.of(csvFile)
                );

                logger.info("Alert sent for domain {} - Slow: {}, Error: {}", domain, slowCount, errorCount);
            }
        } catch (Exception e) {
            logger.error("Error in combined API endpoints scheduler: {}", e.getMessage(), e);
        }
    }

    private File generateCsvForDomain(String domain, List<EndpointDetail> slow, List<EndpointDetail> error) {
        String fileName = "/tmp/api-alert-" + domain.replaceAll("[^a-zA-Z0-9]", "_") + ".csv";
        try (PrintWriter writer = new PrintWriter(new File(fileName))) {
            writer.println("Domain,Endpoint,Type,Value,Count");
            for (EndpointDetail s : slow) {
                writer.printf("%s,%s,SLOW,%dms,%d\n", domain, s.getUrl(), s.getMs(), s.getCount());
            }
            for (EndpointDetail e : error) {
                writer.printf("%s,%s,ERROR,%s,%d\n", domain, e.getUrl(), e.getStatusCode(), e.getCount());
            }
            return new File(fileName);
        } catch (Exception e) {
            logger.error("Error generating CSV for domain {}: {}", domain, e.getMessage());
            return null;
        }
    }
}
