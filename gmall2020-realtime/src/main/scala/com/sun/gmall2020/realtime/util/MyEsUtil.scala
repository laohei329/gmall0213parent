package com.sun.gmall2020.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder

object MyEsUtil {
  private var factory: JestClientFactory = null;

  def main(args: Array[String]): Unit = {
    queryFromEs("movie_index", "movie", "name", "red", "doubanScore")
  }

  def getJestClient: JestClient = {
    if (factory == null) build();
    factory.getObject

  }

  def build() = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop202:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(1000)
      .build()
    )
  }


  /**
   * 通过传入的参数进行查询
   *
   * @param indexName    查询的index
   * @param indexType    查询的index
   * @param searchFilter 查询的属性过滤
   * @param filterName   属性匹配的字段
   * @param sortFilter   按照sortFilter 进行排序
   */
  def queryFromEs(indexName: String, indexType: String, searchFilter: String, filterName: String, sortFilter: String) = {
    val jestClient: JestClient = getJestClient
    val sourceBuilder: SearchSourceBuilder = new SearchSourceBuilder
    sourceBuilder.query(new MatchQueryBuilder(searchFilter, filterName))
    sourceBuilder.sort(sortFilter, SortOrder.ASC)
    sourceBuilder.from(0)
    sourceBuilder.size(20)
    val query: String = sourceBuilder.toString
    //println(query)
    val search: Search = new Search.Builder(query)
      .addIndex(indexName)
      .addType(indexType).build()
    val result: SearchResult = jestClient.execute(search)
    // 查询的返回neirong
    val resultList: util.List[SearchResult#Hit[util.Map[String, Object], Void]] = result.getHits(classOf[util.Map[String, Object]])
    import collection.JavaConversions._
    for (obj <- resultList) {
      val source: util.Map[String, Object] = obj.source
      println(source)
    }
    jestClient.close()
  }

  //批次化操作
  def bulkSave(list: List[(Any, String)], indexName: String) = {

    if (list != null && list.size > 0) {
      val jestClient: JestClient = getJestClient
      val bulkBuilder: Bulk.Builder = new Bulk.Builder
      bulkBuilder.defaultIndex(indexName).defaultType("_doc")
      for ((doc, id) <- list) {
        val index: Index = new Index.Builder(doc).id(id).build()
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()
      val items: util.List[BulkResult#BulkResultItem] = jestClient.execute(bulk).getItems
      println("已保存" + items.size())
      jestClient.close()
    }
  }
}
