package com.gioorgi.pque.client.json;

public interface PQUEJsonProcessor {

    boolean isJson(String json);

    String toJson(Object object);

    <T> T fromJson(String json, Class<T> toClazz);
}
