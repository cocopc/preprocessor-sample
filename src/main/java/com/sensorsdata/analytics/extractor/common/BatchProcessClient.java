package com.sensorsdata.analytics.extractor.common;

import java.util.List;
import java.util.Map;

/**
 * @auther Codievilky August
 * @since 2020/1/3
 */
public interface BatchProcessClient {

  Map<String,String> batchQuery(List<String> keyList);
}
