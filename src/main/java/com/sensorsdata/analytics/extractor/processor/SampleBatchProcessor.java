package com.sensorsdata.analytics.extractor.processor;

import com.sensorsdata.analytics.extractor.common.BatchProcessClient;
import com.sensorsdata.analytics.extractor.common.RecordHandler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @auther Codievilky August
 * @since 2020/1/3
 */
@Slf4j
public class SampleBatchProcessor implements BatchProcessor {
  // 只为接口，没有实际实现。实际的情况可以使 redis-client 等类似的外部模块
  private BatchProcessClient batchProcessClient;
  private ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void process(List<RecordHandler> recordHandlerList) {
    List<String> needQueryKeyList = new ArrayList<>(recordHandlerList.size());
    // 存起来，避免多次的对于数据的解析
    Map<RecordHandler, ObjectNode> parseMap = new HashMap<>();
    for (RecordHandler recordHandler : recordHandlerList) {
      try {
        ObjectNode dataNode = (ObjectNode) objectMapper.readTree(recordHandler.getOriginalData());
        parseMap.put(recordHandler, dataNode);
        JsonNode typeNode = dataNode.get("type");
        if (typeNode != null) {
          // 将想要查询的关键字取出来
          needQueryKeyList.add(typeNode.asText());
        }
      } catch (IOException e) {
        log.warn("parse data failed. ignored");
        // 解析失败，则不进行处理
        recordHandler.send();
      }
    }
    // 只进行一次网络交互
    Map<String, String> queryResult = batchProcessClient.batchQuery(needQueryKeyList);

    for (RecordHandler recordHandler : recordHandlerList) {
      ObjectNode dataNode = parseMap.get(recordHandler);
      // 为空说明之前解析失败了，不去理会
      if (dataNode != null) {
        JsonNode typeNode = dataNode.get("type");
        if (typeNode != null) {
          String queryTypeValue = queryResult.get(typeNode.asText());
          if (queryTypeValue != null) {
            // 将查询后的结果加到数据中
            dataNode.put("type_value", queryTypeValue);
          }
        }
        // 发送生成后的数据
        recordHandler.send(dataNode.toString());
      }
    }
  }
}
