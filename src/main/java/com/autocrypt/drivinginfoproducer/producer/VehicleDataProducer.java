package com.autocrypt.drivinginfoproducer.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Service
@RequiredArgsConstructor
public class VehicleDataProducer {

    private static final String TOPIC = "gps-topic";
    private static final int NUM_VEHICLES = 1; // 생성할 차량 수
    private static final int CONDITION_CHANGE_INTERVAL = 60 * 1000; // 60초

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ExecutorService executorService = Executors.newFixedThreadPool(NUM_VEHICLES);

    @PostConstruct
    public void startProducers() {
        for (int i = 0; i < NUM_VEHICLES; i++) {
            String vehicleId = "vehicle" + (i + 1);
            executorService.submit(() -> produceVehicleData(vehicleId));
        }
    }

    public void produceVehicleData(String vehicleId) {
        double currentLatitude = 37.7749;
        double currentLongitude = -122.4194;
        long lastConditionChangeTime = System.currentTimeMillis();
        String condition = "EMPTY";

        while (true) {
            try {
                // 3% 범위 내에서 변화를 적용
                double latitudeChange = currentLatitude * ThreadLocalRandom.current().nextDouble(-0.03, 0.03);
                double longitudeChange = currentLongitude * ThreadLocalRandom.current().nextDouble(-0.03, 0.03);

                currentLatitude += latitudeChange;
                currentLongitude += longitudeChange;

                long currentTime = System.currentTimeMillis();
                if (currentTime - lastConditionChangeTime >= CONDITION_CHANGE_INTERVAL) {
                    condition = "GUEST";
                    lastConditionChangeTime = currentTime;
                }

                Map<String, Object> message = new HashMap<>();
                message.put("vehicleId", vehicleId);
                message.put("timestamp", Instant.now().atZone(ZoneId.of("Asia/Seoul")).toString());
                message.put("latitude", currentLatitude);
                message.put("longitude", currentLongitude);
                message.put("condition", condition);

                String jsonMessage = objectMapper.writeValueAsString(message);
                kafkaTemplate.send(TOPIC, vehicleId, message);

                System.out.println("Produced message: " + jsonMessage);

                // 5초 대기
                Thread.sleep(5000);

                // condition을 다시 "EMPTY"로 설정
                condition = "EMPTY";
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
