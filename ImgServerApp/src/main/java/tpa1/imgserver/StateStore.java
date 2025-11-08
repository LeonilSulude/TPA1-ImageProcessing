package tpa1.imgserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import java.io.IOException;
import java.util.Map;

public class StateStore {
    private final JedisPool jedisPool;
    private final ObjectMapper mapper = new ObjectMapper();

    public StateStore(String host, int port) {
        this.jedisPool = new JedisPool(host, port);
    }

    public void put(String requestId, Map<String, Object> json) {
        try (Jedis j = jedisPool.getResource()) {
            j.set(requestId, mapper.writeValueAsString(json));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Erro ao serializar JSON para Redis", e);
        }
    }

    public Map<String, Object> get(String requestId) {
        try (Jedis j = jedisPool.getResource()) {
            String v = j.get(requestId);
            if (v == null) return null;
            return mapper.readValue(v, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
        } catch (IOException e) {
            throw new RuntimeException("Erro ao desserializar JSON do Redis", e);
        }
    }
}
