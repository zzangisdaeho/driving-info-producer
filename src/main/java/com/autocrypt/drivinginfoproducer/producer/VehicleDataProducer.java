package com.autocrypt.drivinginfoproducer.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class VehicleDataProducer {

    private static final String TOPIC = "gps-topic";
    private static final int NUM_VEHICLES = 5; // 생성할 차량 수

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ExecutorService executorService = Executors.newFixedThreadPool(NUM_VEHICLES);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(NUM_VEHICLES);

    @PostConstruct
    public void startProducers() {
        for (int i = 0; i < NUM_VEHICLES; i++) {
            String vehicleId = "vehicle" + (i + 1);
            executorService.submit(() -> produceVehicleData(vehicleId));
            scheduleStopMessage(vehicleId);
        }
    }

    public void produceVehicleData(String vehicleId) {
        double currentLatitude = 37.7749;
        double currentLongitude = -122.4194;

        while (true) {
            try {
                // 3% 범위 내에서 변화를 적용
                double latitudeChange = currentLatitude * ThreadLocalRandom.current().nextDouble(-0.03, 0.03);
                double longitudeChange = currentLongitude * ThreadLocalRandom.current().nextDouble(-0.03, 0.03);

                currentLatitude += latitudeChange;
                currentLongitude += longitudeChange;

                Map<String, Object> message = new HashMap<>();
                message.put("vehicleId", vehicleId);
                message.put("timestamp", Instant.now().toString());
                message.put("latitude", currentLatitude);
                message.put("longitude", currentLongitude);

                String jsonMessage = objectMapper.writeValueAsString(message);
                kafkaTemplate.send(TOPIC, vehicleId, message);

                System.out.println("Produced message: " + jsonMessage);

                // 5초 대기
                Thread.sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void scheduleStopMessage(String vehicleId) {
        long initialDelay = ThreadLocalRandom.current().nextLong(40, 61); // 0~40초 사이의 초기 딜레이
        scheduledExecutorService.schedule(() -> produceStopMessage(vehicleId), initialDelay, TimeUnit.SECONDS);
    }

    private void produceStopMessage(String vehicleId) {
        try {
            kafkaTemplate.send(TOPIC, vehicleId, "STOP");

            System.out.println("Produced STOP message: " + vehicleId);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 랜덤한 시간(1~3분) 후에 다시 STOP 메시지를 스케줄링
        long delay = ThreadLocalRandom.current().nextLong(40, 61); // 0~40초 사이의 랜덤 딜레이
        scheduledExecutorService.schedule(() -> produceStopMessage(vehicleId), delay, TimeUnit.SECONDS);
    }
}
