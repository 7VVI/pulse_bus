package com.kronos.pulsBbus.utils;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Properties;

/**
 * @author zhangyh
 * @Date 2025/6/27 16:30
 * @desc
 */
public class PropertiesUtil {

    public static Properties mapToProperties(Map<String, Object> map) {
        Properties props = new Properties();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                props.setProperty(entry.getKey(), entry.getValue().toString());
            }
        }
        return props;
    }

    public static String getString(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value != null ? value.toString() : null;
    }

    public  static  <T> T getWithType(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value != null ? (T)value: null;
    }

    public static String getString(Map<String, Object> map, String key, String defaultValue) {
        String result = getString(map, key);
        return result != null ? result : defaultValue;
    }

    public static Integer getInteger(Map<String, Object> map, String key) {
        Object value = map.get(key);
        try {
            if (value instanceof Integer) return (Integer) value;
            if (value instanceof Number) return ((Number) value).intValue();
            if (value instanceof String) return Integer.parseInt((String) value);
        } catch (Exception ignored) {}
        return null;
    }

    public static Integer getInteger(Map<String, Object> map, String key, Integer defaultValue) {
        Integer result = getInteger(map, key);
        return result != null ? result : defaultValue;
    }

    public static Long getLong(Map<String, Object> map, String key) {
        Object value = map.get(key);
        try {
            if (value instanceof Long) return (Long) value;
            if (value instanceof Number) return ((Number) value).longValue();
            if (value instanceof String) return Long.parseLong((String) value);
        } catch (Exception ignored) {}
        return null;
    }

    public static Long getLong(Map<String, Object> map, String key, Long defaultValue) {
        Long result = getLong(map, key);
        return result != null ? result : defaultValue;
    }

    public static Boolean getBoolean(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof Boolean) return (Boolean) value;
        if (value instanceof String) return Boolean.parseBoolean((String) value);
        if (value instanceof Number) return ((Number) value).intValue() != 0;
        return null;
    }

    public static Boolean getBoolean(Map<String, Object> map, String key, Boolean defaultValue) {
        Boolean result = getBoolean(map, key);
        return result != null ? result : defaultValue;
    }

    public static Double getDouble(Map<String, Object> map, String key) {
        Object value = map.get(key);
        try {
            if (value instanceof Double) return (Double) value;
            if (value instanceof Number) return ((Number) value).doubleValue();
            if (value instanceof String) return Double.parseDouble((String) value);
        } catch (Exception ignored) {}
        return null;
    }

    public static Double getDouble(Map<String, Object> map, String key, Double defaultValue) {
        Double result = getDouble(map, key);
        return result != null ? result : defaultValue;
    }

    public static BigDecimal getBigDecimal(Map<String, Object> map, String key) {
        Object value = map.get(key);
        try {
            if (value instanceof BigDecimal) return (BigDecimal) value;
            if (value instanceof Number) return BigDecimal.valueOf(((Number) value).doubleValue());
            if (value instanceof String) return new BigDecimal((String) value);
        } catch (Exception ignored) {}
        return null;
    }

    public static BigDecimal getBigDecimal(Map<String, Object> map, String key, BigDecimal defaultValue) {
        BigDecimal result = getBigDecimal(map, key);
        return result != null ? result : defaultValue;
    }

    public static <T> T get(Map<String, Object> map, String key, Class<T> clazz) {
        Object value = map.get(key);
        if (clazz.isInstance(value)) {
            return clazz.cast(value);
        }
        return null;
    }
}
